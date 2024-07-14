package org.apache.dolphinscheduler.plugin.registry.consul;

import java.io.IOException;

import okhttp3.ResponseBody;
import retrofit2.Response;

/**
 * 钱维升 创建于 2024/7/13.
 */

public class ConsulException extends RuntimeException {
    private int code;

    /**
     * Constructs an instance of this class.
     *
     * @param message The exception message.
     */
    public ConsulException(String message) {
        super(message);
    }

    /**
     * Constructs an instance of this class.
     *
     * @param message The exception message.
     * @param throwable The wrapped {@link java.lang.Throwable} object.
     */
    public ConsulException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public ConsulException(int code, Response<?> response) {
        super(String.format("Consul request failed with status [%s]: %s",
                code, message(response)));
        this.code = code;
    }

    public ConsulException(Throwable throwable) {
        super("Consul request failed", throwable);
    }

    static String message(Response response) {
        try {
            ResponseBody responseBody = response.errorBody();
            return responseBody == null ? response.message() : responseBody.string();
        } catch (IOException e) {
            return response.message();
        }
    }

    public int getCode() {
        return code;
    }
}
