package com.latam.app;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;


public  class SumBigDecimalFn extends CombineFn<BigDecimal, SumBigDecimalFn.Accum, BigDecimal> {
	
	public static class Accum {
		BigDecimal sum;
		
		public Accum(BigDecimal bigDecimal) {
			// TODO Auto-generated constructor stub
			
			this.sum = bigDecimal;
		}
		
	}
	
	@Override
    public Coder<SumBigDecimalFn.Accum> getAccumulatorCoder(
        CoderRegistry registry, Coder<BigDecimal> inputCoder) {
      return new SumBigDecimalCoder<>();
    }
	
	@Override
	public Accum createAccumulator() {
		return new Accum(new BigDecimal(0));
	}
	
	@Override
	public Accum addInput(Accum accum, BigDecimal input) {
		accum = new Accum(accum.sum.add(input));
		return accum;
	}
	
	@Override
	public Accum mergeAccumulators(Iterable<Accum> accums) {
		Accum merged = createAccumulator();
		for (Accum accum : accums) {
			merged = new Accum(merged.sum.add(accum.sum));
		}
		return merged;
	}

	@Override
	public BigDecimal extractOutput(Accum accum) {
		return  accum.sum;
	}
	
	static class SumBigDecimalCoder<NumT extends Number> extends AtomicCoder<Accum> {
		private static final Coder<BigDecimal> BIGDECIMAL_CODER = BigDecimalCoder.of();

		@Override
		public void encode(Accum value, OutputStream outStream) throws CoderException, IOException {
			BIGDECIMAL_CODER.encode(value.sum, outStream);
		}

		@Override
		public Accum decode(InputStream inStream) throws CoderException, IOException {
			return new Accum(BIGDECIMAL_CODER.decode(inStream));
		}

		@Override
		public void verifyDeterministic() throws NonDeterministicException {
			BIGDECIMAL_CODER.verifyDeterministic();
		}
	}

}