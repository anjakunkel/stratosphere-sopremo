package eu.stratosphere.sopremo.type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * This node represents a long value.
 */
public class LongNode extends AbstractNumericNode implements INumericNode {

	private long value;

	/**
	 * Initializes LongNode.
	 */
	public LongNode() {
		this(0);
	}

	/**
	 * Initializes a LongNode which represents the given <code>long</code>. To create new LongNodes please
	 * use LongNode.valueOf(<code>long</code>) instead.
	 * 
	 * @param value
	 *        the value that should be represented by this node
	 */
	public LongNode(final long value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		TypeFormat.format(this.value, appendable);
	}

	@Override
	public void clear() {
		if (SopremoUtil.DEBUG)
			this.value = 0;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		final long thisVal = this.value, anotherVal = ((LongNode) other).value;
		return thisVal < anotherVal ? -1 : thisVal == anotherVal ? 0 : 1;
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		checkNumber(otherNode);
		this.value = ((INumericNode) otherNode).getLongValue();
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final LongNode other = (LongNode) obj;
		return this.value == other.value;
	}

	@Override
	public BigInteger getBigIntegerValue() {
		return BigInteger.valueOf(this.value);
	}

	@Override
	public BigDecimal getDecimalValue() {
		return BigDecimal.valueOf(this.value);
	}

	@Override
	public double getDoubleValue() {
		return Double.valueOf(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.INumericNode#getGeneralilty()
	 */
	@Override
	public byte getGeneralilty() {
		return 32;
	}

	@Override
	public int getIntValue() {
		return (int) this.value;
	}

	@Override
	public Long getJavaValue() {
		return this.value;
	}

	@Override
	public long getLongValue() {
		return this.value;
	}

	@Override
	public Class<LongNode> getType() {
		return LongNode.class;
	}

	@Override
	public String getValueAsText() {
		return String.valueOf(this.value);
	}

	@Override
	public int hashCode() {
		return (int) (this.value >>> 32) | (int) (this.value & 0xFFFFFFFF);
	}

	@Override
	public boolean isIntegralNumber() {
		return true;
	}

	public void setValue(final long value) {
		this.value = value;
	}

	/**
	 * Creates a new instance of LongNode. This new instance represents the given value.
	 * 
	 * @param value
	 *        the value that should be represented by the new instance
	 * @return the newly created instance of LongNode
	 */
	public static LongNode valueOf(final long value) {
		return new LongNode(value);
	}
}
