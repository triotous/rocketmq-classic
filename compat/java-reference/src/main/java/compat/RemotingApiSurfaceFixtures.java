/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/** Public source/API inventory for the Java remoting response-future capability. */
public final class RemotingApiSurfaceFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private RemotingApiSurfaceFixtures() { }

    private static final class CallbackTrace implements InvokeCallback {
        final List<String> events = new ArrayList<>();
        Throwable failure;

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            events.add("complete");
        }

        @Override
        public void operationFail(Throwable throwable) {
            failure = throwable;
            events.add("fail");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java remoting API fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("responseFuturePublicConstructors", publicConstructors(ResponseFuture.class));
        fixture.put("responseFuturePublicMethods", publicMethods(ResponseFuture.class));
        fixture.put("semaphoreReleaseOnlyOncePublicConstructors",
            publicConstructors(SemaphoreReleaseOnlyOnce.class));
        fixture.put("semaphoreReleaseOnlyOncePublicMethods", publicMethods(SemaphoreReleaseOnlyOnce.class));
        fixture.put("semaphoreReleaseBehavior", semaphoreReleaseBehavior());
        fixture.put("responseFuturePermitReleaseBehavior", responseFuturePermitReleaseBehavior());
        fixture.put("responseFutureCallbackFailureBehavior", responseFutureCallbackFailureBehavior());
        return fixture;
    }

    private static List<String> publicConstructors(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(RemotingApiSurfaceFixtures::constructorSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static List<String> publicMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(RemotingApiSurfaceFixtures::methodSignature)
            .sorted()
            .collect(Collectors.toList());
    }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "(" + parameterTypes(constructor.getParameterTypes()) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getName() + "(" +
            parameterTypes(method.getParameterTypes()) + ")";
    }

    private static String parameterTypes(Class<?>[] types) {
        return String.join(",", Arrays.stream(types).map(Class::getName).collect(Collectors.toList()));
    }

    private static Map<String, Object> semaphoreReleaseBehavior() {
        Semaphore semaphore = new Semaphore(0);
        SemaphoreReleaseOnlyOnce releaseOnce = new SemaphoreReleaseOnlyOnce(semaphore);
        releaseOnce.release();
        releaseOnce.release();

        SemaphoreReleaseOnlyOnce noSemaphore = new SemaphoreReleaseOnlyOnce(null);
        noSemaphore.release();
        noSemaphore.release();
        Map<String, Object> result = new TreeMap<>();
        result.put("returnsSameSemaphore", releaseOnce.getSemaphore() == semaphore);
        result.put("permitsAfterRepeatedRelease", semaphore.availablePermits());
        result.put("nullSemaphoreReturnedAsNull", noSemaphore.getSemaphore() == null);
        result.put("nullSemaphoreReleaseIsNoOp", true);
        return result;
    }

    private static Map<String, Object> responseFuturePermitReleaseBehavior() {
        Semaphore semaphore = new Semaphore(0);
        ResponseFuture future = new ResponseFuture(null, 42, 1000, null,
            new SemaphoreReleaseOnlyOnce(semaphore));
        future.release();
        future.release();
        Map<String, Object> result = new TreeMap<>();
        result.put("opaque", future.getOpaque());
        result.put("permitsAfterRepeatedRelease", semaphore.availablePermits());
        return result;
    }

    private static Map<String, Object> responseFutureCallbackFailureBehavior() {
        Map<String, Object> result = new TreeMap<>();
        Throwable cause = new IllegalStateException("fixture-cause");

        CallbackTrace sendCallback = new CallbackTrace();
        ResponseFuture sendFuture = new ResponseFuture(remoteChannel(), 51,
            RemotingCommand.createRequestCommand(1, null),
            1000, sendCallback, null);
        sendFuture.setCause(cause);
        sendFuture.setSendRequestOK(false);
        sendFuture.executeInvokeCallback();
        result.put("sendFailure", callbackResult(sendCallback, cause));

        CallbackTrace timeoutCallback = new CallbackTrace();
        ResponseFuture timeoutFuture = new ResponseFuture(remoteChannel(), 52,
            RemotingCommand.createRequestCommand(1, null),
            -1, timeoutCallback, null);
        timeoutFuture.setCause(cause);
        timeoutFuture.executeInvokeCallback();
        result.put("timeoutFailure", callbackResult(timeoutCallback, cause));

        CallbackTrace genericCallback = new CallbackTrace();
        ResponseFuture genericFuture = new ResponseFuture(remoteChannel(), 53,
            RemotingCommand.createRequestCommand(1, null),
            Long.MAX_VALUE, genericCallback, null);
        genericFuture.setCause(cause);
        genericFuture.executeInvokeCallback();
        result.put("genericFailure", callbackResult(genericCallback, cause));

        CallbackTrace nullRequestCallback = new CallbackTrace();
        ResponseFuture nullRequestFuture = new ResponseFuture(remoteChannel(), 54, Long.MAX_VALUE,
            nullRequestCallback, null);
        nullRequestFuture.setCause(cause);
        String thrown = null;
        try {
            nullRequestFuture.executeInvokeCallback();
        } catch (Throwable error) {
            thrown = error.getClass().getName();
        }
        Map<String, Object> nullRequest = new TreeMap<>();
        nullRequest.put("throwable", thrown);
        nullRequest.put("callbackEvents", nullRequestCallback.events);
        result.put("nullRequestGenericFailure", nullRequest);
        return result;
    }

    private static Map<String, Object> callbackResult(CallbackTrace callback, Throwable cause) {
        Map<String, Object> result = new TreeMap<>();
        result.put("failureClass", callback.failure == null ? null : callback.failure.getClass().getName());
        result.put("failureCauseSameInstance", callback.failure != null && callback.failure.getCause() == cause);
        result.put("callbackEvents", callback.events);
        return result;
    }

    private static Channel remoteChannel() {
        return (Channel) java.lang.reflect.Proxy.newProxyInstance(
            Channel.class.getClassLoader(),
            new Class<?>[] {Channel.class},
            (proxy, method, args) -> {
                if ("remoteAddress".equals(method.getName())) {
                    return new InetSocketAddress("127.0.0.1", 10911);
                }
                Class<?> returnType = method.getReturnType();
                if (returnType == boolean.class) return false;
                if (returnType == int.class) return 0;
                if (returnType == long.class) return 0L;
                return null;
            });
    }
}
