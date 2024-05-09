package com.bwarelabs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ResultWritable implements Writable {
    private Result result;

    public ResultWritable() {
    }

    public ResultWritable(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return this.result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ClientProtos.Result protoResult = ProtobufUtil.toResult(this.result);

        byte[] bytes = protoResult.toByteArray();

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];

        in.readFully(bytes);

        ClientProtos.Result protoResult = ClientProtos.Result.parseFrom(bytes);
        this.result = ProtobufUtil.toResult(protoResult);
    }
}
