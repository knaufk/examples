package com.immerok.cloud.examples.events;

import java.util.Objects;

/** A message object that Flink recognizes as a valid POJO. */
public class Message {

    public String user;
    public String message;

    /** A Flink POJO must have a no-args default constructor */
    public Message() {}

    public Message(String user, String message) {
        this.user = user;
        this.message = message;
    }

    /** Used for printing during development */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message other = (Message) o;
        return user.equals(other.user) && message.equals(other.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, message);
    }

    @Override
    public String toString() {
        return "Message{" + "user='" + user + '\'' + ", message='" + message + '\'' + '}';
    }
}
