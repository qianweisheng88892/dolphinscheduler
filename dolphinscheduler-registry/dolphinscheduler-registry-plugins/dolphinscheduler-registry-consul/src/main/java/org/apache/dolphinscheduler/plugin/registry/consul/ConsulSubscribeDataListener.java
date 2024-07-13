package org.apache.dolphinscheduler.plugin.registry.consul;

import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import io.vertx.core.Handler;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.WatchResult;

public class ConsulSubscribeDataListener implements Handler<WatchResult<KeyValueList>> {

    private final SubscribeListener listener;

    public ConsulSubscribeDataListener(SubscribeListener listener) {
        this.listener = listener;
    }

    Map<String, KeyValue> lastValues = null;


    @Override
    public void handle(WatchResult<KeyValueList> kvList) {
        KeyValueList keyValueList = kvList.nextResult();
        Map<String, KeyValue> newValues = keyValueList.getList().stream().collect(Collectors.toMap(KeyValue::getKey, i -> i));
        if (lastValues == null) {
            lastValues = newValues;
        } else {
            List<KeyValue> addedData = new ArrayList<>();
            List<KeyValue> deletedData = new ArrayList<>();
            List<KeyValue> updatedData = new ArrayList<>();
            for (Map.Entry<String, KeyValue> entry : newValues.entrySet()) {
                KeyValue newData = entry.getValue();
                KeyValue oldData = lastValues.get(entry.getKey());
                if (oldData == null) {
                    addedData.add(newData);
                } else {
                    if (entry.getValue().getModifyIndex() != oldData.getModifyIndex()) {
                        updatedData.add(newData);
                    }
                }
            }
            for (Map.Entry<String, KeyValue> entry : lastValues.entrySet()) {
                if (!newValues.containsKey(entry.getKey())) {
                    deletedData.add(entry.getValue());
                }
            }
            lastValues = newValues;
            // trigger listener
            triggerListener(addedData, listener, Event.Type.ADD);
            triggerListener(deletedData, listener, Event.Type.REMOVE);
            triggerListener(updatedData, listener, Event.Type.UPDATE);
        }
    }

    private void triggerListener(List<KeyValue> list, SubscribeListener listener, Event.Type type) {
        for (KeyValue val : list) {
            listener.notify(new Event(val.getKey(), val.getKey(), val.getValue(), type));
        }
    }


}
