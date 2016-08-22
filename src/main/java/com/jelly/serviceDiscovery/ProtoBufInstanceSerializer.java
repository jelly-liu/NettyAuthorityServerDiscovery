package com.jelly.serviceDiscovery;

import com.jelly.util.ProtoStuffSerializer;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

/**
 * Created by jelly on 2016-8-22.
 */
public class ProtoBufInstanceSerializer<T> implements InstanceSerializer<T> {
    private final ObjectMapper mapper;
    private final Class<T> payloadClass;
    private final JavaType type;

    public ProtoBufInstanceSerializer(Class<T> payloadClass) {
        this.payloadClass = payloadClass;
        mapper = new ObjectMapper();
        type = mapper.getTypeFactory().constructType(ServiceInstance.class);
    }

    @Override
    public byte[] serialize(ServiceInstance<T> instance) throws Exception {
        return ProtoStuffSerializer.serialize(instance);
    }

    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        return ProtoStuffSerializer.deserialize(bytes, ServiceInstance.class);
    }
}
