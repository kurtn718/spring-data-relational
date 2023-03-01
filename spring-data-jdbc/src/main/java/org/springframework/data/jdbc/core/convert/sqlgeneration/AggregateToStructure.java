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

import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

class AggregateToStructure {

	private final JdbcMappingContext context;

	AggregateToStructure(JdbcMappingContext context) {
		this.context = context;
	}

	AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select createSelectStructure(
			RelationalPersistentEntity<?> aggregateRoot) {

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension> builder = new AnalyticStructureBuilder<>();

		builder.addTable(aggregateRoot, td -> configureTableDefinition(aggregateRoot, td));
		addReferencedEntities(builder, aggregateRoot);

		return builder.build().getSelect();
	}

	private void addReferencedEntities(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension> builder,
			RelationalPersistentEntity<?> currentEntity) {

		currentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
			if (p.isEntity()) {
				RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(p.getActualType());
				builder.addChildTo(p.getOwner(), entity, td2 -> configureTableDefinition(entity, td2));
			}
		});
	}

	private AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition configureTableDefinition(
			RelationalPersistentEntity<?> aggregateRoot,
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition td) {

		for (PersistentPropertyPath<RelationalPersistentProperty> persistentPropertyPath : context
				.findPersistentPropertyPaths(aggregateRoot.getType(),
						(RelationalPersistentProperty rpp) -> rpp.getOwner().equals(aggregateRoot))) { // TODO: we are currently
																																														// only handling a single
																																														// level aggregate

			RelationalPersistentProperty p = persistentPropertyPath.getLeafProperty();
			PersistentPropertyPathExtension extPath = new PersistentPropertyPathExtension(context, persistentPropertyPath);
			if (p.isIdProperty()) {
				td = td.withId(extPath);
			} else {
				if (!p.isEntity()) {
					td = td.withColumns(extPath);
				}
			}
		}
		return td;
	}
}
