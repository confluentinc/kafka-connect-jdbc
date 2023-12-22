package io.confluent.connect.jdbc.gp.gpss;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import io.confluent.connect.jdbc.gp.gpss.api.*;
/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.4.0)",
    comments = "Source: gpss.proto")
public final class GpssGrpc {

  private GpssGrpc() {}

  public static final String SERVICE_NAME = "io.confluent.connect.jdbc.gp.gpss.api.Gpss";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<ConnectRequest,
      Session> METHOD_CONNECT =
      io.grpc.MethodDescriptor.<ConnectRequest, Session>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "Connect"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              ConnectRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              Session.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<Session,
      com.google.protobuf.Empty> METHOD_DISCONNECT =
      io.grpc.MethodDescriptor.<Session, com.google.protobuf.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "Disconnect"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              Session.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.protobuf.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<OpenRequest,
      com.google.protobuf.Empty> METHOD_OPEN =
      io.grpc.MethodDescriptor.<OpenRequest, com.google.protobuf.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "Open"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              OpenRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.protobuf.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<WriteRequest,
      com.google.protobuf.Empty> METHOD_WRITE =
      io.grpc.MethodDescriptor.<WriteRequest, com.google.protobuf.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "Write"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              WriteRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.google.protobuf.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<CloseRequest,
      TransferStats> METHOD_CLOSE =
      io.grpc.MethodDescriptor.<CloseRequest, TransferStats>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "Close"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              CloseRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              TransferStats.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<ListSchemaRequest,
      Schemas> METHOD_LIST_SCHEMA =
      io.grpc.MethodDescriptor.<ListSchemaRequest, Schemas>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "ListSchema"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              ListSchemaRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              Schemas.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<ListTableRequest,
      Tables> METHOD_LIST_TABLE =
      io.grpc.MethodDescriptor.<ListTableRequest, Tables>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "ListTable"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              ListTableRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              Tables.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<DescribeTableRequest,
      Columns> METHOD_DESCRIBE_TABLE =
      io.grpc.MethodDescriptor.<DescribeTableRequest, Columns>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "api.Gpss", "DescribeTable"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              DescribeTableRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              Columns.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GpssStub newStub(io.grpc.Channel channel) {
    return new GpssStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GpssBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GpssBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GpssFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GpssFutureStub(channel);
  }

  /**
   */
  public static abstract class GpssImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public void connect (ConnectRequest request,
        io.grpc.stub.StreamObserver<Session> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CONNECT, responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public void disconnect (Session request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DISCONNECT, responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public void open (OpenRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_OPEN, responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public void write (WriteRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_WRITE, responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public void close (CloseRequest request,
        io.grpc.stub.StreamObserver<TransferStats> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CLOSE, responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public void listSchema (ListSchemaRequest request,
        io.grpc.stub.StreamObserver<Schemas> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_SCHEMA, responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public void listTable (ListTableRequest request,
        io.grpc.stub.StreamObserver<Tables> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_TABLE, responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public void describeTable (DescribeTableRequest request,
        io.grpc.stub.StreamObserver<Columns> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DESCRIBE_TABLE, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_CONNECT,
            asyncUnaryCall(
              new MethodHandlers<
                ConnectRequest,
                Session>(
                  this, METHODID_CONNECT)))
          .addMethod(
            METHOD_DISCONNECT,
            asyncUnaryCall(
              new MethodHandlers<
                Session,
                com.google.protobuf.Empty>(
                  this, METHODID_DISCONNECT)))
          .addMethod(
            METHOD_OPEN,
            asyncUnaryCall(
              new MethodHandlers<
                OpenRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_OPEN)))
          .addMethod(
            METHOD_WRITE,
            asyncUnaryCall(
              new MethodHandlers<
                WriteRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_WRITE)))
          .addMethod(
            METHOD_CLOSE,
            asyncUnaryCall(
              new MethodHandlers<
                CloseRequest,
                TransferStats>(
                  this, METHODID_CLOSE)))
          .addMethod(
            METHOD_LIST_SCHEMA,
            asyncUnaryCall(
              new MethodHandlers<
                ListSchemaRequest,
                Schemas>(
                  this, METHODID_LIST_SCHEMA)))
          .addMethod(
            METHOD_LIST_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                ListTableRequest,
                Tables>(
                  this, METHODID_LIST_TABLE)))
          .addMethod(
            METHOD_DESCRIBE_TABLE,
            asyncUnaryCall(
              new MethodHandlers<
                DescribeTableRequest,
                Columns>(
                  this, METHODID_DESCRIBE_TABLE)))
          .build();
    }
  }

  /**
   */
  public static final class GpssStub extends io.grpc.stub.AbstractStub<GpssStub> {
    private GpssStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GpssStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GpssStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public void connect (ConnectRequest request,
        io.grpc.stub.StreamObserver<Session> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CONNECT, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public void disconnect (Session request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DISCONNECT, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public void open (OpenRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_OPEN, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public void write (WriteRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_WRITE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public void close (CloseRequest request,
        io.grpc.stub.StreamObserver<TransferStats> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CLOSE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public void listSchema (ListSchemaRequest request,
        io.grpc.stub.StreamObserver<Schemas> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_SCHEMA, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public void listTable (ListTableRequest request,
        io.grpc.stub.StreamObserver<Tables> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public void describeTable (DescribeTableRequest request,
        io.grpc.stub.StreamObserver<Columns> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DESCRIBE_TABLE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class GpssBlockingStub extends io.grpc.stub.AbstractStub<GpssBlockingStub> {
    private GpssBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GpssBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GpssBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public Session connect (ConnectRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CONNECT, getCallOptions(), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.protobuf.Empty disconnect (Session request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DISCONNECT, getCallOptions(), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.protobuf.Empty open (OpenRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_OPEN, getCallOptions(), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.protobuf.Empty write (WriteRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_WRITE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public TransferStats close (CloseRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CLOSE, getCallOptions(), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public Schemas listSchema (ListSchemaRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_SCHEMA, getCallOptions(), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public Tables listTable (ListTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_TABLE, getCallOptions(), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public Columns describeTable (DescribeTableRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DESCRIBE_TABLE, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class GpssFutureStub extends io.grpc.stub.AbstractStub<GpssFutureStub> {
    private GpssFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GpssFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GpssFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GpssFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Establish a connection to Greenplum Database; returns a Session object
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Session> connect(
        ConnectRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CONNECT, getCallOptions()), request);
    }

    /**
     * <pre>
     * Disconnect, freeing all resources allocated for a session
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> disconnect(
        Session request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DISCONNECT, getCallOptions()), request);
    }

    /**
     * <pre>
     * Prepare and open a table for write
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> open(
        OpenRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_OPEN, getCallOptions()), request);
    }

    /**
     * <pre>
     * Write data to table
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> write(
        WriteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_WRITE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Close a write operation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<TransferStats> close(
        CloseRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CLOSE, getCallOptions()), request);
    }

    /**
     * <pre>
     * List all available schemas in a database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Schemas> listSchema(
        ListSchemaRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_SCHEMA, getCallOptions()), request);
    }

    /**
     * <pre>
     * List all tables and views in a schema
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Tables> listTable(
        ListTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_TABLE, getCallOptions()), request);
    }

    /**
     * <pre>
     * Decribe table metadata(column name and column type)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<Columns> describeTable(
        DescribeTableRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DESCRIBE_TABLE, getCallOptions()), request);
    }
  }

  private static final int METHODID_CONNECT = 0;
  private static final int METHODID_DISCONNECT = 1;
  private static final int METHODID_OPEN = 2;
  private static final int METHODID_WRITE = 3;
  private static final int METHODID_CLOSE = 4;
  private static final int METHODID_LIST_SCHEMA = 5;
  private static final int METHODID_LIST_TABLE = 6;
  private static final int METHODID_DESCRIBE_TABLE = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GpssImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GpssImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECT:
          serviceImpl.connect( (ConnectRequest) request,
              (io.grpc.stub.StreamObserver<Session>) responseObserver);
          break;
        case METHODID_DISCONNECT:
          serviceImpl.disconnect( (Session) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open( (OpenRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write( (WriteRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CLOSE:
          serviceImpl.close( (CloseRequest) request,
              (io.grpc.stub.StreamObserver<TransferStats>) responseObserver);
          break;
        case METHODID_LIST_SCHEMA:
          serviceImpl.listSchema( (ListSchemaRequest) request,
              (io.grpc.stub.StreamObserver<Schemas>) responseObserver);
          break;
        case METHODID_LIST_TABLE:
          serviceImpl.listTable( (ListTableRequest) request,
              (io.grpc.stub.StreamObserver<Tables>) responseObserver);
          break;
        case METHODID_DESCRIBE_TABLE:
          serviceImpl.describeTable( (DescribeTableRequest) request,
              (io.grpc.stub.StreamObserver<Columns>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class GpssDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return GpssOuterClass.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GpssGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GpssDescriptorSupplier())
              .addMethod(METHOD_CONNECT)
              .addMethod(METHOD_DISCONNECT)
              .addMethod(METHOD_OPEN)
              .addMethod(METHOD_WRITE)
              .addMethod(METHOD_CLOSE)
              .addMethod(METHOD_LIST_SCHEMA)
              .addMethod(METHOD_LIST_TABLE)
              .addMethod(METHOD_DESCRIBE_TABLE)
              .build();
        }
      }
    }
    return result;
  }
}
