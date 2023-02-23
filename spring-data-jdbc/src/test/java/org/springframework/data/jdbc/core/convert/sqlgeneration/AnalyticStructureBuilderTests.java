/*
 * Copyright 2022 the original author or authors.
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

import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticAssertions.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticJoinPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticViewPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ConditionPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ForeignKeyPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.GreatestPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.LiteralPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.MaxOverPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.RowNumberPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.TableDefinitionPattern.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class AnalyticStructureBuilderTests {

	/**
	 * A simple table should result in a simple select. Columns are represented by
	 * {@link org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticStructureBuilder.BaseColumn} since they are
	 * directly referenced.
	 */
	@Test
	void simpleTableWithColumns() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>() //
				.addTable("person", td -> td.withId("person_id").withColumns("value", "lastname")) //
				.build();

		assertThat(builder).hasExactColumns("person_id", "value", "lastname") //
				.hasId("person_id") //
				.hasStructure(td("person"));

	}

	@Test
	void simpleTableWithColumnsAddedInMultipleSteps() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>() //
				.addTable("person", td -> td.withId("person_id").withColumns("value").withColumns("lastname")) //
				.build();

		assertThat(builder).hasExactColumns("person_id", "value", "lastname") //
				.hasId("person_id") //
				.hasStructure(td("person"));

	}

	@Test
	void tableWithSingleChild() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parent-value", "parent-lastname"))
				.addChildTo("parent", "child", td -> td.withColumns("child-value", "child-lastname")) //
				.build();

		assertThat(builder).hasExactColumns( //
				"parentId", "parent-value", "parent-lastname", //
				"child-value", "child-lastname", //
				fk("child", "parentId"), //
				greatest("parentId", fk("child", "parentId")), //
				rn(fk("child", "parentId")), //
				greatest(lit(1), rn(fk("child", "parentId"))) //
		).hasId("parentId") //
				.hasStructure(aj(td("parent"), av(td("child")), //
						eq("parentId", fk("child", "parentId")), // <-- should fail due to wrong column value
						eq(lit(1), rn(fk("child", "parentId"))) //
				));
	}

	@Test
	void tableWithSingleChildWithKey() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "child", td -> td.withColumns("childName", "childLastname").withKeyColumn("childKey")) //
				.build();

		assertThat(builder) //
				.hasExactColumns("parentId", "parentName", "parentLastname", //
						fk("child", "parentId"), //
						greatest("parentId", fk("child", "parentId")), //
						rn(fk("child", "parentId")), //
						greatest(lit(1), rn(fk("child", "parentId"))), //
						"childKey", "childName", "childLastname") //
				.hasId("parentId") //
				.hasStructure( //
						aj( //
								td("parent"), //
								av(td("child")), //
								eq("parentId", fk("child", "parentId")), //
								eq(lit(1), rn(fk("child", "parentId"))) //
						) //
				); //
	}

	@Test
	void tableWithMultipleChildren() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "child1", td -> td.withColumns("childName", "childLastname"))
				.addChildTo("parent", "child2", td -> td.withColumns("siblingName", "siblingLastName")) //
				.build();

		GreatestPattern<LiteralPattern> rnChild1 = greatest(lit(1), rn(fk("child1", "parentId")));

		assertThat(builder) //
				.hasExactColumns("parentId", "parentName", "parentLastname", //
						fk("child1", "parentId"), //
						greatest("parentId", fk("child1", "parentId")), //
						rn(fk("child1", "parentId")), //
						rnChild1, //
						"childName", "childLastname", //

						fk("child2", "parentId"), //
						greatest("parentId", fk("child2", "parentId")), //
						rn(fk("child2", "parentId")), //
						greatest(rnChild1, rn(fk("child2", "parentId"))), //
						"siblingName", //
						"siblingLastName")
				.hasId("parentId") //
				.hasStructure( //
						aj( //
								aj(td("parent"), av(td("child1")), //
										eq("parentId", fk("child1", "parentId")), //
										eq(lit(1), rn(fk("child1", "parentId"))) //
								), //
								av(td("child2")), //
								eq("parentId", fk("child2", "parentId")), //
								eq(rnChild1, rn(fk("child2", "parentId"))) //
						));
	}

	@Nested
	class TableWithChainOfChildren {

		@Test
		void middleChildHasId() {

			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			assertThat(builder) //
					.hasExactColumns( //
							// child
							"childName", //
							fk("child", "parentId"), // join by FK
							rn(fk("child", "parentId")), // join by RN <-- not found, but really should be there

							// parent
							"parentId", "parentName", //
							fk("parent", "grannyId"), // join

							// child + parent
							greatest("parentId", fk("child", "parentId")), // completed parentId for joining with granny, only
																															// necessary for joining with further children?
							greatest(lit(1), rn(fk("child", "parentId"))), // completed RN for joining with granny
							maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId"))),

							// granny table
							"grannyId", "grannyName", //
							// (parent + child) --> granny
							greatest("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), // completed
																																																											// grannyId
							greatest(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // completed RN for granny.

					) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fk("child", "parentId")), //
											eq(lit(1), rn(fk("child", "parentId"))) //
									), //
									eq("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), //
									eq(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // corrected
							) //
					);
		}

		/**
		 * middle children that don't have an id, nor a key can't referenced by further children
		 */
		@Test
		void middleChildHasNoId() {

			// assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			builder.getSelect();
		}

		@Test
		void middleChildWithKeyHasId() {

			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent",
							td -> td.withId("parentId").withKeyColumn("parentKey").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			ForeignKeyPattern<String, String> fkChildToParent = fk("child", "parentId");
			GreatestPattern<String> idOfJoinParentWithChild = greatest("parentId", fkChildToParent);
			ForeignKeyPattern<String, String> fkParentToGranny = fk("parent", "grannyId");
			GreatestPattern<LiteralPattern> rnJoinParentWithChild = greatest(lit(1), rn(fkParentToGranny));
			MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinParentWithChildToGranny = maxOver(fkParentToGranny,
					idOfJoinParentWithChild);

			assertThat(builder).hasExactColumns( //

					// child columns
					"childName", //
					fkChildToParent, //

					// parent
					"parentId", "parentKey", "parentName", //
					fkParentToGranny, //

					// join of parent + child
					rn(fkChildToParent), // rownumber for the join itself. should be in the result because it is a single
																// valued indicator if a child is present in this row. Relevant when there are
																// siblings
					idOfJoinParentWithChild, // guarantees a parent id in all rows and may serve as a join
																		// column for siblings of child.
					fkJoinParentWithChildToGranny, //

					// granny
					"grannyId", "grannyName", //
					// join of granny + (parent + child)
					greatest("grannyId", fkJoinParentWithChildToGranny), // grannyId
					// for every column
					rnJoinParentWithChild, //
					greatest(lit(1), greatest(lit(1), rn(fkChildToParent))) //
			) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fkChildToParent), //
											eq(lit(1), rn(fkChildToParent)) //
									), //
									eq("grannyId", fkJoinParentWithChildToGranny), //
									eq(lit(1), rnJoinParentWithChild) //
							) //
					);
		}

		@Test
		void middleChildWithKeyHasNoId() {

			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withColumns("parentName").withKeyColumn("parentKey"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					fk("parent", "grannyId"), //
					greatest(lit(1), greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", "parentKey")))),
					greatest("grannyId",
							maxOver(fk("parent", "grannyId"),
									greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
									greatest("parentKey", fk("child", "parentKey")))),
					maxOver(fk("parent", "grannyId"), greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
							greatest("parentKey", fk("child", "parentKey"))),
					"parentKey", "parentName", //

					fk("child", fk("parent", "grannyId")), //
					greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
					fk("child", "parentKey"), //
					greatest("parentKey", fk("child", "parentKey")), //
					rn(fk("child", fk("parent", "grannyId")), fk("child", "parentKey")), //
					greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", "parentKey"))), //
					"childName" //
			) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
											eq("parentKey", fk("child", "parentKey")), //
											eq(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", "parentKey"))) //
									), //
									eq("grannyId",
											maxOver(fk("parent", "grannyId"),
													greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
													greatest("parentKey", fk("child", "parentKey")))), //
									eq(lit(1), greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", "parentKey")))) //
							) //
					);
		}

		@Test
		void middleSingleChildHasId() {

			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					fk("parent", "grannyId"), //
					greatest("grannyId", fk("parent", "grannyId")), //
					rn(fk("parent", "grannyId")), //
					greatest(lit(1), rn(fk("parent", "grannyId"))), //
					"parentId", "parentName", //
					fk("child", "parentId"), // <--- not found
					greatest("parentId", fk("child", "parentId")), //
					rn(fk("child", "parentId")), //
					greatest(lit(1), rn(fk("child", "parentId"))), // <--- not found
					"childName" //
			) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fk("child", "parentId")), //
											eq(lit(1), rn(fk("child", "parentId"))) //
									), //
									eq("grannyId", fk("parent", "grannyId")), //
									eq(lit(1), rn(fk("parent", "grannyId"))) //
							) //
					);
		}

		@Test
		void middleSingleChildHasNoId() {

			AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "parent", td -> td.withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName")) //
					.build();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					fk("parent", "grannyId"), //
					greatest("grannyId", fk("parent", "grannyId")), // TODO: max is superfluous for single
					rn(fk("parent", "grannyId")), //
					greatest(lit(1), rn(fk("parent", "grannyId"))), //
					"parentName", //
					fk("child", fk("parent", "grannyId")), //
					greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
					rn(fk("child", fk("parent", "grannyId"))), //
					greatest(lit(1), rn(fk("child", fk("parent", "grannyId")))), //
					"childName") //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
											eq(lit(1), rn(fk("child", fk("parent", "grannyId")))) //
									), //
									eq("grannyId", fk("parent", "grannyId")), //
									eq(lit(1), rn(fk("parent", "grannyId"))) //
							) //
					);
		}

	}

	@Test
	void mediumComplexHierarchy() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer", "address", td -> td.withId("addressId").withColumns("addressName")) //
				.addChildTo("address", "city", td -> td.withColumns("cityName")) //
				.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("address", "type", td -> td.withColumns("typeName")) //
				.build();

		GreatestPattern<LiteralPattern> rnOrder = greatest(lit(1), rn(fk("address", "customerId")));
		GreatestPattern<LiteralPattern> rnCity = greatest(lit(1), rn(fk("city", "addressId")));
		assertThat(builder).hasExactColumns( //
				"customerId", "customerName", //
				fk("address", "customerId"), //
				greatest("customerId", fk("address", "customerId")), //
				rn(fk("address", "customerId")), //
				rnOrder, //
				"addressId", "addressName", // <--

				fk("city", "addressId"), //
				greatest("addressId", fk("city", "addressId")), //
				rn(fk("city", "addressId")), //
				rnCity, //
				"cityName", //

				fk("order", "customerId"), //
				greatest("customerId", fk("order", "customerId")), //
				rn(fk("order", "customerId")), //
				greatest(rnOrder, rn(fk("order", "customerId"))), //
				"orderId", "orderName", //

				fk("type", "addressId"), //
				greatest("addressId", fk("type", "addressId")), //
				rn(fk("type", "addressId")), //
				greatest(rnCity, rn(fk("type", "addressId"))), //
				"typeName"//
		).hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("address"), //
														av(td("city")), //
														eq("addressId", fk("city", "addressId")), //
														eq(lit(1), rn(fk("city", "addressId"))) //
												), //
												av(td("type")), //
												eq("addressId", fk("type", "addressId")), //
												eq(rnCity, rn(fk("type", "addressId"))) //
										), //
										eq("customerId", fk("address", "customerId")), //
										eq(lit(1), rn(fk("address", "customerId"))) //
								), //
								av(td("order")), //
								eq("customerId", fk("order", "customerId")), //
								eq(rnOrder, rn(fk("order", "customerId"))) //
						) //
				);

	}

	@Test
	void mediumComplexHierarchy2() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer", "keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName")) //
				.addChildTo("keyAccount", "assistant", td -> td.withColumns("assistantName")) //
				.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("keyAccount", "office", td -> td.withColumns("officeName")) //
				.addChildTo("order", "item", td -> td.withId("itemId").withColumns("itemName")) //
				.build();

		assertThat(builder).hasStructure( //
				aj( //
						aj( //
								td("customer"), //
								aj( //
										aj( //
												td("keyAccount"), //
												av(td("assistant")), //
												eq("keyAccountId", fk("assistant", "keyAccountId")), //
												eq(lit(1), rn(fk("assistant", "keyAccountId"))) //
										), //
										av(td("office")), //
										eq("keyAccountId", fk("office", "keyAccountId")), //
										eq(greatest(lit(1), rn(fk("assistant", "keyAccountId"))), rn(fk("office", "keyAccountId"))) //
								), //
								eq("customerId", fk("keyAccount", "customerId")), //
								eq(lit(1), rn(fk("keyAccount", "customerId"))) //
						), //
						aj( //
								td("order"), //
								av(td("item")), //
								eq("orderId", fk("item", "orderId")), //
								eq(lit(1), rn(fk("item", "orderId"))) //
						), //
						eq("customerId", fk("order", "customerId")), //
						eq(greatest(lit(1), rn(fk("keyAccount", "customerId"))), rn(fk("order", "customerId"))) //
				) //
		);

	}

	@Test
	void complexHierarchy() {

		AnalyticStructure<String, String> builder = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer", "keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName")) //
				.addChildTo("keyAccount", "assistant", td -> td.withColumns("assistantName")) //
				.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("keyAccount", "office", td -> td.withId("officeId").withColumns("officeName")) //
				.addChildTo("order", "item", td -> td.withColumns("itemName")) //
				.addChildTo("order", "shipment", td -> td.withColumns("shipmentName")) //
				.addChildTo("office", "room", td -> td.withColumns("roomNumber")) //
				.build();

		GreatestPattern<LiteralPattern> rnKeyAccount = greatest(lit(1), rn(fk("keyAccount", "customerId")));
		GreatestPattern<LiteralPattern> rnAssistant = greatest(lit(1), rn(fk("assistant", "keyAccountId")));

		GreatestPattern<LiteralPattern> rnItem = greatest(lit(1), rn(fk("item", "orderId")));
		assertThat(builder).hasExactColumns( //
				"customerId", "customerName", //
				fk("keyAccount", "customerId"), //
				greatest("customerId", fk("keyAccount", "customerId")), //
				rn(fk("keyAccount", "customerId")), //
				rnKeyAccount, //
				"keyAccountId", "keyAccountName", //

				fk("assistant", "keyAccountId"), //
				greatest("keyAccountId", fk("assistant", "keyAccountId")), //
				rn(fk("assistant", "keyAccountId")), //
				rnAssistant, //
				"assistantName", //

				fk("office", "keyAccountId"), //
				greatest("keyAccountId", fk("office", "keyAccountId")), //
				rn(fk("office", "keyAccountId")), //
				greatest(rnAssistant, rn(fk("office", "keyAccountId"))), //
				"officeName", //

				fk("order", "customerId"), //
				greatest("customerId", fk("order", "customerId")), //
				rn(fk("order", "customerId")), //
				greatest(rnKeyAccount, rn(fk("order", "customerId"))), //
				"orderId", "orderName", //

				fk("item", "orderId"), //
				greatest("orderId", fk("item", "orderId")), //
				rn(fk("item", "orderId")), //
				rnItem, //
				"itemName", //

				fk("shipment", "orderId"), //
				greatest("orderId", fk("shipment", "orderId")), //
				rn(fk("shipment", "orderId")), //
				greatest(rnItem, rn(fk("shipment", "orderId"))), //
				"shipmentName", "officeId", //

				fk("room", "officeId"), //
				greatest("officeId", fk("room", "officeId")), //
				rn(fk("room", "officeId")), //
				greatest(lit(1), rn(fk("room", "officeId"))), //
				"roomNumber" //
		).hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("keyAccount"), //
														av(td("assistant")), //
														eq("keyAccountId", fk("assistant", "keyAccountId")), //
														eq(lit(1), rn(fk("assistant", "keyAccountId"))) //
												), //
												aj( //
														td("office"), //
														av(td("room")), //
														eq("officeId", fk("room", "officeId")), //
														eq(lit(1), rn(fk("room", "officeId"))) //
												), //
												eq("keyAccountId", fk("office", "keyAccountId")), //
												eq(rnAssistant, rn(fk("office", "keyAccountId"))) //
										), //
										eq("customerId", fk("keyAccount", "customerId")), //
										eq(lit(1), rn(fk("keyAccount", "customerId"))) //
								), //
								aj( //
										aj( //
												td("order"), //
												av(td("item")), //
												eq("orderId", fk("item", "orderId")), //
												eq(lit(1), rn(fk("item", "orderId"))) //
										), //
										av(td("shipment")), //
										eq("orderId", fk("shipment", "orderId")), //
										eq(rnItem, rn(fk("shipment", "orderId"))) //
								), //
								eq("customerId", fk("order", "customerId")), //
								eq(rnKeyAccount, rn(fk("order", "customerId"))) //
						) //
				);

	}
}
