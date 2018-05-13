/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package ta.nemahuta.neo4j.structure;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.types.Entity;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JGraphFactoryWhileLoadingProvider {

    public static class NoConstructorArgumentsElementIdAdapter implements Neo4JElementIdAdapter<Long> {

        @Override
        public Long generate() {
            return null;
        }

        @Override
        public String propertyName() {
            return null;
        }

        @Override
        public Long retrieveId(Entity entity) {
            return null;
        }

        @Override
        public Long convert(Object id) {
            return null;
        }

        @Override
        public String matchPredicateOperand(String alias) {
            return null;
        }
    }

    public static class ConstructorDriverArgumentElementIdAdapter implements Neo4JElementIdAdapter<Long> {

        public ConstructorDriverArgumentElementIdAdapter(Driver driver) {
            Assert.assertNotNull("Driver cannot be null", driver);
        }

        @Override
        public Long generate() {
            return null;
        }

        @Override
        public String propertyName() {
            return null;
        }

        @Override
        public Long retrieveId(Entity entity) {
            return null;
        }

        @Override
        public Long convert(Object id) {
            return null;
        }

        @Override
        public String matchPredicateOperand(String alias) {
            return null;
        }
    }

    @Mock
    private Driver driver;

    @Test
    public void givenProviderWithNoArgumentsConstructorShouldCreateInstance() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        // act
        Neo4JElementIdAdapter<?> provider = Neo4JGraphFactory.loadProvider(driver, NoConstructorArgumentsElementIdAdapter.class.getName());
        // assert
        Assert.assertNotNull("Failed to create provider instance", provider);
    }

    @Test
    public void givenProviderWithDriverArgumentConstructorShouldCreateInstance() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        // act
        Neo4JElementIdAdapter<?> provider = Neo4JGraphFactory.loadProvider(driver, ConstructorDriverArgumentElementIdAdapter.class.getName());
        // assert
        Assert.assertNotNull("Failed to create provider instance", provider);
    }
}
