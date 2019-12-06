package me.m1key.streams.bank;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class BankBalanceStreamTest {
    @Test
    public void shouldExtractNameCorrectly() {
        String exampleValue = "{\"Name\":\"Bob\", \"amount\":770, \"time\":\"2019-12-06T13:02:41\"}";
        assertThat(BankBalanceStream.extractName(exampleValue), equalTo("Bob"));
    }
    @Test
    public void shouldExtractAmountCorrectly() {
        String exampleValue = "{\"Name\":\"Bob\", \"amount\":770, \"time\":\"2019-12-06T13:02:41\"}";
        assertThat(BankBalanceStream.extractAmount(exampleValue), equalTo(770));
    }

}