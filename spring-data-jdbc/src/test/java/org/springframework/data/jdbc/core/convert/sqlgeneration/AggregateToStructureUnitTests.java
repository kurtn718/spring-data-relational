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

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

class AggregateToStructureUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();

	AggregateToStructure ats = new AggregateToStructure(context);
	RelationalPersistentEntity<?> rootEntity;

	@Test
	void simpleTable() {

		rootEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select = ats
				.createSelectStructure(rootEntity);

		AnalyticAssertions.assertThat(select) //
				.hasExactColumns( //
						path("id"), //
						path("aColumn"))
				.isInstanceOf(AnalyticStructureBuilder.TableDefinition.class) //
				.hasId(new PersistentPropertyPathExtension(context,
						context.getPersistentPropertyPath("id", rootEntity.getType())));
	}

	@Test
	void singleReference() {

		rootEntity = context.getRequiredPersistentEntity(SingleReference.class);
		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select = ats
				.createSelectStructure(rootEntity);

		AnalyticAssertions.assertThat(select) //
				.hasAtLeastColumns( //
						path("id"), //
						path("dummy.id"), //
						path("dummy.aColumn") //
				).isInstanceOf(AnalyticStructureBuilder.AnalyticJoin.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select child = ((AnalyticStructureBuilder.AnalyticJoin) select)
				.getChild();

		AnalyticAssertions.assertThat(child)
				.hasExactColumns( //
						path("dummy.id"), //
						path("dummy.aColumn") //
				);
	}

	private PersistentPropertyPathExtension path(String path) {
		return new PersistentPropertyPathExtension(context, context.getPersistentPropertyPath(path, rootEntity.getType()));
	}

	static class DummyEntity {
		@Id Long id;

		String aColumn;
	}

	static class SingleReference {
		@Id Long id;
		DummyEntity dummy;
	}
}
