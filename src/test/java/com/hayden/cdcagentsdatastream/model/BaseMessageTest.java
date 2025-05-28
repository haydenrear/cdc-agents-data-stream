package com.hayden.cdcagentsdatastream.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static com.hayden.shared.assertions.AssertionUtil.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class BaseMessageTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testBaseMessageConstructor() throws JsonProcessingException {
        String hello = objectMapper.writeValueAsString(BaseMessage.AIMessage.builder().name("hello")
                .content(new BaseMessage.ContentValue("hello")).build());
        BaseMessage h = objectMapper.readValue(hello, BaseMessage.class);
        assertInstanceOf(BaseMessage.AIMessage.class, h);
        assertThat(h.getContent().content().size()).isEqualTo(1);
        assertThat(h.getContent().content()).isEqualTo(List.of("hello"));
        hello = objectMapper.writeValueAsString(BaseMessage.AIMessage.builder().name("hello")
                .content(new BaseMessage.ContentValue(List.of("hello", "goodbye"))).build());
        h = objectMapper.readValue(hello, BaseMessage.class);
        assertInstanceOf(BaseMessage.AIMessage.class, h);
        assertThat(h.getContent().content().size()).isEqualTo(2);
        assertThat(h.getContent().content()).isEqualTo(List.of("hello", "goodbye"));
    }

}