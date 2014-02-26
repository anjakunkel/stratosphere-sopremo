package eu.stratosphere.sopremo.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.dag.GraphLevelPartitioner;
import eu.stratosphere.util.dag.GraphLevelPartitioner.Level;

/**
 * The class <code>CompositeOperatorTest</code> contains tests for the class <code>{@link CompositeOperator}</code>.
 */
public class CompositeOperatorTest extends EqualCloneTest<CompositeOperatorTest.CompositeOperatorImpl> {

	@Test
	public void testAsElementaryOperators() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperator<?> fixture = new CompositeOperatorImpl(1);
		fixture.setInputs(input1, input2, input3);

		final ElementarySopremoModule module = fixture.asElementaryOperators();

		assertNotNull(module);
		final List<Level<Operator<?>>> reachableNodes = GraphLevelPartitioner.getLevels(
			module.getAllOutputs(), OperatorNavigator.INSTANCE);
		assertEquals(3, reachableNodes.get(0).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(1).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(2).getLevelNodes().size());
		assertEquals(1, reachableNodes.get(3).getLevelNodes().size());

		for (int index = 0; index < 3; index++)
			assertSame(Source.class, reachableNodes.get(0).getLevelNodes().get(index).getClass());
		assertSame(ElementaryOperatorImpl.class, reachableNodes.get(1)
			.getLevelNodes().get(0).getClass());
		assertSame(ElementaryOperatorImpl.class, reachableNodes.get(2)
			.getLevelNodes().get(0).getClass());
		assertSame(Sink.class, reachableNodes.get(3).getLevelNodes().get(0).getClass());
	}
	

	@Test
	public void testNoPropagation() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperator<?> fixture = new CompositeOperatorImpl(1);
		fixture.setInputs(input1, input2, input3);


		final ElementarySopremoModule module = fixture.asElementaryOperators();
		assertNotNull(module);
		fixture.propagateAllProperties(module);
		
		for(Operator<?> op: module.getReachableNodes()) {
			assertNotNull(op);
		}
		
	}

	@Test
	public void testPropagation() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperatorImplWithProperty fixture = new CompositeOperatorImplWithProperty(1);
		fixture.setInputs(input1, input2, input3);
		fixture.setPropertyX(new String("Test"));
		final ElementarySopremoModule module = fixture.asElementaryOperators();
		assertNotNull(module);
		
		for(Operator<?> op: module.getReachableNodes()) {
			assertNotNull(op);
			if(op instanceof ElementaryOperatorImpl) {
				Object propertyX = ((ElementaryOperatorImpl) op).getPropertyX();
				assertEquals(fixture.getPropertyX(), propertyX);
			}
		}
		
	}
	@Test
	public void testPropagationOfUnsetValue() throws Exception {
		final Operator<?> input1 = new Source("file://1");
		final Operator<?> input2 = new Source("file://2");
		final Operator<?> input3 = new Source("file://3");
		final CompositeOperatorImplWithProperty fixture = new CompositeOperatorImplWithProperty(1);
		fixture.setInputs(input1, input2, input3);
		final ElementarySopremoModule module = fixture.asElementaryOperators();
		assertNotNull(module);
		
		for(Operator<?> op: module.getReachableNodes()) {
			assertNotNull(op);
			if(op instanceof ElementaryOperatorImpl) {
				Object propertyX = ((ElementaryOperatorImpl) op).getPropertyX();
				assertEquals(fixture.getPropertyX(), propertyX);
			}
		}
		
	}

	@Override
	protected CompositeOperatorImpl createDefaultInstance(final int index) {
		return new CompositeOperatorImpl(index);
	}
	
	static class CompositeOperatorImplWithProperty extends CompositeOperatorImpl {
		
		protected Object value;
		
		public CompositeOperatorImplWithProperty(final int index) {
			super(index);
		}
		
		@Property(propagate = true)
		public void setPropertyX(Object value) {
			this.value = value;
		}
		
		public Object getPropertyX() {
			return this.value;
		}
	}


	static class CompositeOperatorImpl extends CompositeOperator<CompositeOperatorImpl> {
		private final int index;

		/**
		 * Initializes CompositeOperatorTest.CompositeOperatorImpl.
		 */
		public CompositeOperatorImpl() {
			this(0);
		}

		public CompositeOperatorImpl(final int index) {
			super(3, 1);
			this.index = index;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.
		 * stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public void addImplementation(final SopremoModule module) {
			module.embed(new ElementaryOperatorImpl().withInputs(null,
				new ElementaryOperatorImpl().withInputs(null, null)));
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (!(obj instanceof CompositeOperatorImpl))
				return false;
			final CompositeOperatorImpl other = (CompositeOperatorImpl) obj;
			if (this.index != other.index)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

	}

	@InputCardinality(min = 2, max = 2)
	static class ElementaryOperatorImpl extends ElementaryOperator<ElementaryOperatorImpl> {
		protected Object value;
		
		@Property
		public void setPropertyX(Object value) {
			this.value = value;
		}
		
		public Object getPropertyX() {
			return this.value;
		}
		static class Implementation extends SopremoCross {
			/*
			 * (non-Javadoc)
			 * @see
			 * eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere
			 * .sopremo.type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void cross(final IJsonNode value1, final IJsonNode value2, final JsonCollector<IJsonNode> out) {
			}
		}
	}
}