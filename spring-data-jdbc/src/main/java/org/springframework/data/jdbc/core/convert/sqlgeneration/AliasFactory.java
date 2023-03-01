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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class AliasFactory {

	public static final int MAX_NAME_HINT_LENGTH = 20;
	HashMap<Object, String> cache = new HashMap<>();
	private int tableIndex = 0;
	private int columnIndex = 0;
	private int fkIndex = 0;

	private final List<SingleAliasFactory> factories = Arrays.asList(
			new SingleAliasFactory<>(AnalyticStructureBuilder.AnalyticView.class, "V"),
			new SingleAliasFactory<>(AnalyticStructureBuilder.ForeignKey.class, "FK"),
			new SingleAliasFactory<>(AnalyticStructureBuilder.RowNumber.class, "RN"),
			new SingleAliasFactory<>(RelationalPersistentProperty.class, "C", RelationalPersistentProperty::getName),
			new SingleAliasFactory<>(RelationalPersistentEntity.class, "T", rpe -> rpe.getTableName().toString()));

	String getAliasFor(Object key) {

		String values = cache.computeIfAbsent(key, k -> createAlias(k));

		return values;
	}

	private String createAlias(Object key) {

		if (key instanceof AnalyticStructureBuilder.TableDefinition tableDefinition) {
			return createAlias(tableDefinition.getTable());
		}

		for (SingleAliasFactory factory : factories) {
			if (factory.applies(key)) {
				return factory.next(key);
			}
		}

		throw new UnsupportedOperationException("can't generate alias for " + key);
	}

	private String sanitize(String baseTableName) {

		String lettersOnly = baseTableName.replaceAll("[^A-Za-z]+", "");
		return lettersOnly.toUpperCase().substring(0, Math.min(MAX_NAME_HINT_LENGTH, lettersOnly.length()));
	}

	private class SingleAliasFactory<T> {
		private final Class<T> type;
		private final String prefix;
		private final Function<T, String> suffixFunction;
		private int index = 0;

		private SingleAliasFactory(Class<T> type, String prefix) {
			this(type, prefix, t -> "");
		}

		private SingleAliasFactory(Class<T> type, String prefix, Function<T, String> suffixFunction) {

			this.type = type;
			this.prefix = prefix;
			this.suffixFunction = suffixFunction;
		}

		boolean applies(Object key) {
			return type.isInstance(key);
		}

		String next(Object key) {

			if (!type.isInstance(key)) {
				throw new IllegalStateException("This SingleAliasFactory only supports keys of type " + type);
			}

			String suffix = suffix((T) key);
			if (!suffix.isEmpty()) {
				suffix = "_" + sanitize(suffix);
			}
			return prefix + "%04d".formatted(++index) + suffix;
		}

		String suffix(T t) {
			return suffixFunction.apply(t);
		}
	}
}
