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
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

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
		return createSelect(
				(AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select) analyticView
						.getFroms().get(0));
	}

	private SelectBuilder.SelectFromAndJoinCondition createJoin(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin) {

		AnalyticStructureBuilder.Select child = analyticJoin.getChild();
		SelectBuilder.BuildSelect childSelect = createSelect(child);
		InlineQuery childQuery = InlineQuery.create(childSelect.build(), getAliasFor(child));

		Collection<Expression> columns = getSelectList(child, childQuery, false);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent = analyticJoin
				.getParent();
		TableLike parentTable = tableFor(parent);
		columns.addAll(getSelectList(parent, parentTable, false));

		SelectBuilder.SelectFromAndJoin selectAndParent = StatementBuilder.select(columns).from(parentTable);

		Condition condition = createJoinCondition(analyticJoin);
		return selectAndParent.fullOuterJoin(childQuery).on(condition);
	}

	private String getAliasFor(Object object) {
		return aliasFactory.getAliasFor(object);
	}

	private Collection<Expression> getSelectList(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent,
			TableLike parentTable, boolean declareAlias) {

		Collection<Expression> tableColumns = new ArrayList<>();

		for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn : parent
				.getColumns()) {

			if (analyticColumn instanceof AnalyticStructureBuilder.ForeignKey fk) {
				System.out.println("for now skipping foreign key" + fk);
				continue;
			}
			Expression column;

			PersistentPropertyPathExtension property = analyticColumn.getColumn();
			if (property == null) {
				// TODO: handle all the special join management columns.
				if (analyticColumn instanceof AnalyticStructureBuilder.RowNumber rn) {

					Column[] partitionBys = ((Stream<Column>) rn.getPartitionBy().stream()
							.map(ac -> parentTable.column(aliasFactory.getAliasFor(ac)))).toArray(Column[]::new);

					column = AnalyticFunction.create("ROW_NUMBER").partitionBy(partitionBys).as(aliasFactory.getAliasFor(rn));
				} else {
					throw new UnsupportedOperationException("Can't handle " + analyticColumn);
				}
			} else {
				column = createColumn(parentTable, property, declareAlias);
			}

			tableColumns.add(column);
		}
		return tableColumns;
	}

	private Column createColumn(TableLike parentTable, PersistentPropertyPathExtension path, boolean declareAlias) {

		String alias = getAliasFor(path);

		if (declareAlias) {

			return parentTable //
					.column(path.getColumnName()) //
					.as(alias);
		} else {
			return parentTable.column(alias);
		}
	}

	private TableLike tableFor(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent) {

		if (parent instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition) {
			return Table.create(tableDefinition.getTable().getTableName()).as(getAliasFor(parent));
		}

		throw new UnsupportedOperationException("can only create table names for TableDefinitions right now");
	}

	private Condition createJoinCondition(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin) {

		for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.JoinCondition joinCondition : analyticJoin
				.getConditions()) {
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn left = joinCondition
					.getLeft();
			joinCondition.getRight();

			return Conditions.isEqual(Expressions.just("1"), Expressions.just("2"));
		}
		return null;
	}

	private SelectBuilder.SelectFromAndJoin createSimpleSelect(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition) {

		List<? extends AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn> analyticColumns = tableDefinition
				.getColumns();

		Collection<Expression> tableColumns = new ArrayList<>();
		TableLike table = tableFor(tableDefinition);
		for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn : analyticColumns) {

			if (analyticColumn instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

				SqlIdentifier columnName = createFkColumnName(foreignKey);
				String alias = aliasFactory.getAliasFor(foreignKey);
				tableColumns.add(table.column(columnName).as(alias));
				continue;
			}
			PersistentPropertyPathExtension property = analyticColumn.getColumn();
			if (property == null) {
				// TODO: handle all the special join management columns.
				continue;
			}
			SqlIdentifier columnName = property.getColumnName();
			Column column = table.column(columnName).as(getAliasFor(property));
			tableColumns.add(column);

		}

		SelectBuilder.SelectFromAndJoin from = StatementBuilder.select(tableColumns).from(table);
		return from;
	}

	private static SqlIdentifier createFkColumnName(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

		return foreignKey.getForeignKeyColumn().getColumn().getReverseColumnName();
	}
}
