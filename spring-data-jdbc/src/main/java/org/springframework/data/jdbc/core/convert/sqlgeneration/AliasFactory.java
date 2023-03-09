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

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class AliasFactory {

	public static final int MAX_NAME_HINT_LENGTH = 20;
	HashMap<Object, String> cache = new HashMap<>();
	private int tableIndex = 0;
	private int columnIndex = 0;
	private int fkIndex = 0;

	private final List<SingleAliasFactory> factories = Arrays.asList(
			new DelegatingAliasFactory<>(AnalyticStructureBuilder.TableDefinition.class, td -> td.getTable()),
			new DefaultAliasFactory<>(AnalyticStructureBuilder.ForeignKey.class, "FK"),
			new DefaultAliasFactory<>(AnalyticStructureBuilder.AnalyticView.class, "V"),
			new DefaultAliasFactory<>(AnalyticStructureBuilder.RowNumber.class, "RN"),
			new DelegatingAliasFactory<>(PersistentPropertyPathExtension.class, pppe -> pppe.getRequiredPersistentPropertyPath()),
			new DelegatingAliasFactory<>(PersistentPropertyPath.class, ppp -> ppp.getRequiredLeafProperty()),
			new DelegatingAliasFactory<>(AnalyticStructureBuilder.BaseColumn.class, (AnalyticStructureBuilder.BaseColumn bc) -> bc.getColumn()),
			new DefaultAliasFactory<>(RelationalPersistentProperty.class, "C", pp -> pp.getName()),
			new DefaultAliasFactory<>(RelationalPersistentEntity.class, "T", rpe -> rpe.getTableName().toString()),
			new DefaultAliasFactory<>(AnalyticStructureBuilder.Greatest.class, "GT", gt -> {
				if (gt.getRight().getColumn() instanceof AnalyticStructureBuilder.RowNumber)
					return "RN";
				if (gt.getLeft() instanceof AnalyticStructureBuilder.Literal)
					return "RN";
				if (gt.getLeft() instanceof AnalyticStructureBuilder.BaseColumn) {
					return "FK";
				}
				return "unknown";
			})
	);

	String getAliasFor(Object key) {

		String cachedAlias = cache.get(key);
		if (cachedAlias != null) {
			return cachedAlias;
		}

		for (SingleAliasFactory factory : factories) {
			if (factory.applies(key)) {
				if (factory instanceof DefaultAliasFactory<?> daf) {
					String alias = daf.next(key);
					cache.put(key, alias);
					return alias;
				}
				if (factory instanceof DelegatingAliasFactory<?> daf) {
					String alias = getAliasFor(daf.getDelegateKey(key));
					cache.put(key, alias);
					return alias;
				}
				throw new IllegalStateException("AliasFactory of type %s is not supported".formatted(factory.getClass().toString()));
			}
		}

		throw new UnsupportedOperationException("can't generate alias for " + key + " of type " + key.getClass());
	}

	private String sanitize(String baseTableName) {

		String lettersOnly = baseTableName.replaceAll("[^A-Za-z]+", "");
		return lettersOnly.toUpperCase().substring(0, Math.min(MAX_NAME_HINT_LENGTH, lettersOnly.length()));
	}

	private abstract class SingleAliasFactory<T> {
		final Class<T> type;

		private SingleAliasFactory(Class<T> type) {
			this.type = type;
		}

		boolean applies(Object key) {
			return type.isInstance(key);
		}
	}

	private class DefaultAliasFactory<T> extends SingleAliasFactory<T> {
		private final String prefix;
		private final Function<T, String> suffixFunction;
		private int index = 0;

		private DefaultAliasFactory(Class<T> type, String prefix) {
			this(type, prefix, t -> "");
		}

		private DefaultAliasFactory(Class<T> type, String prefix, Function<T, String> suffixFunction) {

			super(type);
			this.prefix = prefix;
			this.suffixFunction = suffixFunction;
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

	private class DelegatingAliasFactory<T> extends SingleAliasFactory<T> {
		private final Function<T, Object> delegateKey;

		private DelegatingAliasFactory(Class<T> type, Function<T, Object> delegateKey) {

			super(type);

			this.delegateKey = delegateKey;
		}

		private Object getDelegateKey(Object key) {
			return delegateKey.apply((T)key);
		}
	}
}
