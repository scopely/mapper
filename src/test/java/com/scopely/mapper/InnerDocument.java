package com.scopely.mapper;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = InnerDocument.Builder.class)
public interface InnerDocument {
    String getRequiredInnerValue();

    Optional<Long> getOptionalRequiredValue();

    class Builder extends InnerDocument_Builder {}
}
