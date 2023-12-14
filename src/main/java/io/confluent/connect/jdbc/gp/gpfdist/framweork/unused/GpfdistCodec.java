//
//package io.confluent.connect.jdbc.gp.gpfdist.framweork;
//
//import reactor.fn.Consumer;
//import reactor.fn.Function;
//import reactor.io.buffer.Buffer;
//import reactor.io.codec.Codec;
//
//import java.nio.ByteBuffer;
//import java.nio.charset.Charset;
//
///**
// * Gpfdist related reactor {@link Codec}.
// *
// */
//public class GpfdistCodec extends Codec<Buffer, Buffer, Buffer> {
//
//	final byte[] h1 = Character.toString('D').getBytes(Charset.forName("UTF-8"));
//
//	@SuppressWarnings("resource")
//	@Override
//	public Buffer apply(Buffer t) {
//			byte[] h2 = ByteBuffer.allocate(4).putInt(t.flip().remaining()).array();
//			return new Buffer().append(h1).append(h2).append(t).flip();
//	}
//
//	@Override
//	public Function<Buffer, Buffer> decoder(Consumer<Buffer> next) {
//		return null;
//	}
//
//}
