package com.jelly.util;

import io.protostuff.GraphIOUtil;
import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ConcurrentHashMap;

public class ProtoStuffSerializer {
    private static final Logger log = LoggerFactory.getLogger(ProtoStuffSerializer.class);
    private final static ConcurrentHashMap<Class<?>, Schema<?>> cachedSchema = new ConcurrentHashMap<>();

    private  static <T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>) cachedSchema.get(clazz);
        if (schema == null) {
            schema = RuntimeSchema.createFrom(clazz);
            cachedSchema.put(clazz, schema);
        }
        return schema;
    }

    public static <T> byte[] serialize( final T source ) {
        final Class<T> clazz = (Class<T>) source.getClass();
        final LinkedBuffer buffer = LinkedBuffer.allocate();
        try {
            final Schema<T> schema = getSchema( clazz );
            ByteArrayOutputStream temp = new ByteArrayOutputStream();
            GraphIOUtil.writeTo(temp, source, schema, buffer);
            return temp.toByteArray();
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
        finally {
            buffer.clear();
        }
        return null;
    }

    public static <T> T deserialize( final byte[] bytes , final Class<T> clazz ) {
        try {
            Schema<T> schema = getSchema(clazz);
            final T result = (T)schema.newMessage();
            GraphIOUtil.mergeFrom(bytes, result, schema);
            return result;
        }
        catch ( final Exception e ) {
            e.printStackTrace();
        }
        return null;
    }
}
