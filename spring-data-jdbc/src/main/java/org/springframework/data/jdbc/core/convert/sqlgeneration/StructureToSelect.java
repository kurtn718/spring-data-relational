/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.jdbc.core.convert.sqlgeneration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.*;

class StructureToSelect {

	private final AliasFactory aliasFactory;

	StructureToSelect() {
		this(new AliasFactory());
	}

	StructureToSelect(AliasFactory aliasFactory) {
		this.aliasFactory = aliasFactory;
	}

	SelectBuilder.BuildSelect createSelect(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure) {

		SelectBuilder.BuildSelect unorderedSelect = createUnorderedSelect(queryStructure);

		if (queryStructure instanceof AnalyticStructureBuilder.AnalyticJoin join) {

			Collection<OrderByField> orderByFields = new ArrayList<>();
			join.getId().forEach(id -> orderByFields.add(OrderByField.from(createAliasExpression((AnalyticStructureBuilder.DerivedColumn) id))));

			orderByFields.add(OrderByField.from(createAliasExpression(join.getRowNumber())));
			return ((SelectBuilder.SelectFromAndJoinCondition) unorderedSelect).orderBy(orderByFields);
		}

		return unorderedSelect;
	}

	private SelectBuilder.BuildSelect createUnorderedSelect(AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure) {
		if (queryStructure instanceof AnalyticStructureBuilder.TableDefinition tableDefinition) {
			return createSimpleSelect(tableDefinition);
		}

		if (queryStructure instanceof AnalyticStructureBuilder.AnalyticJoin analyticJoin) {
			SelectBuilder.SelectFromAndJoinCondition join = createJoin(analyticJoin);
			return join;
		}

		if (queryStructure instanceof AnalyticStructureBuilder.AnalyticView analyticView) {
			return createView(analyticView);
		}

		throw new UnsupportedOperationException("Can't convert " + queryStructure);
	}

	private SelectBuilder.BuildSelect createView(AnalyticStructureBuilder.AnalyticView analyticView) {
		return createSimpleSelect(analyticView);
	}

	private SelectBuilder.SelectFromAndJoinCondition createJoin(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin) {

		AnalyticStructureBuilder.Select child = analyticJoin.getChild();
		SelectBuilder.BuildSelect childSelect = createUnorderedSelect(child);
		InlineQuery childQuery = InlineQuery.create(childSelect.build(), getAliasFor(child));

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent = analyticJoin
				.getParent();
		TableLike parentTable = tableFor(parent);
		Collection<Expression> columns = createSelectExpressionList(analyticJoin.getColumns(), parentTable);

		SelectBuilder.SelectFromAndJoin selectAndParent = StatementBuilder.select(columns).from(parentTable);

		Condition condition = createJoinCondition(parentTable, childQuery, analyticJoin);
		return selectAndParent.fullOuterJoin(childQuery).on(condition);
	}

	private String getAliasFor(Object object) {
		return aliasFactory.getAliasFor(object);
	}

	// TODO: table is not required when the column is derived
	private Expression createColumn(TableLike table,
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn) {

		if (analyticColumn instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

			SqlIdentifier columnName = createFkColumnName(foreignKey);
			String alias = getAliasFor(foreignKey);
			return table.column(columnName).as(alias);
		}
		if (analyticColumn instanceof AnalyticStructureBuilder.DerivedColumn derivedColumn) {
			return createAliasExpression(derivedColumn);
		}
		if (analyticColumn instanceof AnalyticStructureBuilder.Literal al) {
			return SQL.literalOf(al.getValue());
		}
		PersistentPropertyPathExtension property = analyticColumn.getColumn();
		if (property != null) {
			return createSimpleColumn(table, property);
		}
		if (analyticColumn instanceof AnalyticStructureBuilder.RowNumber rn) {

			return createRownumberExpression(table, rn);
		}
		if (analyticColumn instanceof AnalyticStructureBuilder.Greatest gt) {

			Expression leftColumn = createColumn(table, gt.left);
			Expression rightColumn = createColumn(table, gt.right);

			return SimpleFunction.create("GREATEST", Arrays.asList(leftColumn, rightColumn)).as(getAliasFor(gt));
		} else {
			throw new UnsupportedOperationException("Can't handle " + analyticColumn);
		}

	}

	private Expression createAliasExpression(AnalyticStructureBuilder.DerivedColumn derivedColumn) {
		return Expressions.just(aliasFactory.getAliasFor(derivedColumn.column));
	}

	private Expression createRownumberExpression(TableLike parentTable,
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.RowNumber rn) {
		Expression column;
		Expression[] partitionBys = (rn.getPartitionBy().stream().map(ac -> createColumn(parentTable, ac)))
				.toArray(Expression[]::new);

		column = AnalyticFunction.create("ROW_NUMBER").partitionBy(partitionBys).as(getAliasFor(rn));
		return column;
	}

	private Column createSimpleColumn(TableLike parentTable, PersistentPropertyPathExtension path) {

		String alias = getAliasFor(path);

		return parentTable //
				.column(path.getColumnName()) //
				.as(alias);
	}

	private TableLike tableFor(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent) {

		if (parent instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition) {
			return Table.create(tableDefinition.getTable().getTableName()).as(getAliasFor(parent));
		}

		if (parent instanceof AnalyticStructureBuilder.AnalyticView av) {
			return tableFor((AnalyticStructureBuilder.TableDefinition) av.getParent());
		}

		throw new UnsupportedOperationException("can only create table names for TableDefinitions right now");
	}

	private Condition createJoinCondition(TableLike parentTable, TableLike childQuery,
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin) {

		Condition condition = null;
		for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.JoinCondition joinCondition : analyticJoin
				.getConditions()) {

			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn left = joinCondition
					.getLeft();
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn right = joinCondition
					.getRight();

			Comparison newCondition = Conditions.isEqual(createColumn(parentTable, left), createColumn(childQuery, right));
			if (condition == null) {
				condition = newCondition;
			} else {
				condition = condition.and(newCondition);
			}
		}
		return condition;
	}

	private SelectBuilder.SelectFromAndJoin createSimpleSelect(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select) {

		List<? extends AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn> analyticColumns = select
				.getColumns();

		TableLike table = tableFor(select);

		Collection<Expression> selectExpressionList = createSelectExpressionList(analyticColumns, table);

		SelectBuilder.SelectFromAndJoin from = StatementBuilder.select(selectExpressionList).from(table);
		return from;
	}

	private Collection<Expression> createSelectExpressionList(
			List<? extends AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn> analyticColumns,
			TableLike table) {
		Collection<Expression> selectExpressionList = new ArrayList<>();
		for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn : analyticColumns) {
			selectExpressionList.add(createColumn(table, analyticColumn));
		}
		return selectExpressionList;
	}

	private static SqlIdentifier createFkColumnName(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

		return foreignKey.getForeignKeyColumn().getColumn().getReverseColumnName();
	}
}
