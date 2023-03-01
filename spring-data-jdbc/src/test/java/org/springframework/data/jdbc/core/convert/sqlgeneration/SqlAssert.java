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

import static org.assertj.core.api.Assertions.*;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class SqlAssert extends AbstractAssert<SqlAssert, Statement> {

	private AliasFactory aliasFactory;

	protected SqlAssert(Statement actual, Class<?> selfType) {
		super(actual, selfType);
	}

	static SqlAssert assertThatParsed(String actualSql) {

		try {
			Statement parsed = CCJSqlParserUtil.parse(actualSql);
			return new SqlAssert(parsed, SqlAssert.class);
		} catch (JSQLParserException e) {
			Assertions.fail("Couldn't parse '%s'".formatted(actualSql));
		}

		throw new IllegalStateException("This should be unreachable");
	}

	public SqlAssert hasColumns(ColumnsSpec columnsSpec) {

		List<ActualColumn> actualColumns = collectActualColumns();

		List<ColumnSpec> notFound = new ArrayList<>();

		// check normal property based columns
		columnsSpec.foreach(aliasFactory, (ColumnSpec columnSpec) -> {
			for (ActualColumn currentColumn : actualColumns) {
				if (columnSpec.matches(currentColumn)) {
					actualColumns.remove(currentColumn);
					return;
				}
			}
			notFound.add(columnSpec);
		});

		if (actualColumns.isEmpty() && notFound.isEmpty()) {
			return this;
		}

		String failureMessage = "Expected %s%n to contain columns representing%n %s %n but ";
		if (!notFound.isEmpty()) {
			failureMessage += "no columns for %s were found";
		}
		if (!notFound.isEmpty() && !actualColumns.isEmpty()) {
			failureMessage += "%n and ";
		}
		if (!actualColumns.isEmpty()) {
			failureMessage += "the columns %s where not expected.";
		}

		String notFoundString = notFound.stream().map(key -> {
			if (key instanceof PropertyBasedColumn pbc) {
				return columnsSpec.paths.get(pbc.property);
			} else {
				return key.toString();
			}
		}).collect(Collectors.joining(", "));

		if (!notFound.isEmpty() && !actualColumns.isEmpty()) {
			throw failure(failureMessage, getSelect().toString(), columnsSpec, notFoundString, actualColumns);
		}
		if (!notFound.isEmpty()) {
			throw failure(failureMessage, getSelect().toString(), columnsSpec, notFoundString);
		} else {
			throw failure(failureMessage, getSelect().toString(), columnsSpec, actualColumns);
		}

	}

	public void assignsAliasesExactlyOnce() {

		// TODO: this should eventually test that all inner columns are aliased exactly once when they are originally
		// selected.
		List<ActualColumn> aliasedColumns = new ArrayList<>();

		List<ActualColumn> actualColumns = collectActualColumns();
		for (ActualColumn column : actualColumns) {
			// TODO: this should be a "is simple column" method in the ActualColumn class
			if (!column.alias.isEmpty() && !column.expression.startsWith("ROW_NUMBER() OVER (PARTITION BY ")) {
				aliasedColumns.add(column);
			}
		}

		assertThat(aliasedColumns)
				.describedAs("The columns %s should not have aliases, but %s have.", actualColumns, aliasedColumns).isEmpty();
	}

	private List<ActualColumn> collectActualColumns() {

		List<ActualColumn> actualColumns = new ArrayList<>();
		for (SelectItem selectItem : getSelect().getSelectItems()) {
			selectItem.accept(new SelectItemVisitorAdapter() {
				@Override
				public void visit(SelectExpressionItem item) {
					actualColumns.add(new ActualColumn(item.getExpression().toString(),
							item.getAlias() == null ? "" : item.getAlias().toString()));
				}
			});
		}
		return actualColumns;
	}

	public SqlAssert selectsFrom(String tableName) {

		assertThat(getSelect().getFromItem().toString()) //
				.isEqualTo(tableName);

		return this;
	}

	private PlainSelect getSelect() {
		return (PlainSelect) ((Select) actual).getSelectBody();
	}

	static ColumnsSpec from(RelationalMappingContext context, RelationalPersistentEntity<?> entity) {
		return new ColumnsSpec(context, entity);
	}

	public SqlAssert withAliases(AliasFactory aliasFactory) {
		this.aliasFactory = aliasFactory;
		return this;
	}

	static class ColumnsSpec {
		private final RelationalMappingContext context;
		private final RelationalPersistentEntity<?> currentEntity;
		final Map<RelationalPersistentProperty, String> paths = new HashMap<>();

		final List<Object> specialColumns = new ArrayList<>();

		public ColumnsSpec(RelationalMappingContext context, RelationalPersistentEntity<?> entity) {

			this.context = context;
			this.currentEntity = entity;
		}

		public ColumnsSpec property(String pathString) {

			PersistentPropertyPath<RelationalPersistentProperty> path = context.getPersistentPropertyPath(pathString,
					currentEntity.getType());

			RelationalPersistentProperty leafProperty = path.getLeafProperty();
			paths.put(leafProperty, pathString);

			return this;
		}

		public ColumnsSpec rowNumber(String dummy) {

			specialColumns.add("row number");
			return this;
		}

		public void foreach(AliasFactory aliasFactory, Consumer<? super ColumnSpec> specConsumer) {

			paths.keySet().forEach(p -> specConsumer.accept(new PropertyBasedColumn(aliasFactory, p)));
			specialColumns.forEach(o -> specConsumer.accept(new RowNumberSpec(aliasFactory, o)));
		}

		@Override
		public String toString() {
			return paths.values().toString();
		}
	}

	private interface ColumnSpec {
		boolean matches(ActualColumn actualColumn);
	}

	private record PropertyBasedColumn(AliasFactory aliasFactory,
			RelationalPersistentProperty property) implements ColumnSpec {

		@Override
		public boolean matches(ActualColumn actualColumn) {

			String alias = aliasFactory.getAliasFor(property);
			if (actualColumn.alias.equals(alias) || actualColumn.column.equals(alias)) {
				return true;
			}
			return false;
		}

	}

	private record RowNumberSpec(AliasFactory aliasFactory, Object someObject) implements ColumnSpec {

		@Override
		public boolean matches(ActualColumn actualColumn) {
			return actualColumn.expression.startsWith("ROW_NUMBER() OVER (PARTITION BY");
		}

	}

	private record ActualColumn(String expression, String table, String column, String alias) {
		ActualColumn(String expression, String alias) {
			this(expression, table(expression), column(expression), alias.replace(" AS ", ""));
		}

		private static String table(String expression) {

			int index = expression.indexOf(".");
			if (index > 0) {
				return expression.substring(0, index);
			}
			return expression;
		}

		static String column(String expression) {

			int index = expression.indexOf(".");
			int spaceIndex = expression.indexOf(" ");
			if (index > 0 && (spaceIndex < 0 || index < spaceIndex)) {
				return expression.substring(index + 1);
			}
			return expression;
		}

		@Override
		public String toString() {
			return expression + (alias == null || alias.isBlank() ? "" : (" AS " + alias));
		}
	}

}
