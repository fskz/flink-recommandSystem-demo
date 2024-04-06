package com.demo.map;

import com.demo.domain.LogEntity;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class UserInterestMapFunction extends RichMapFunction<LogEntity, Tuple2<LogEntity, Integer>> {

    ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100L))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("Action Time", Integer.class);
        desc.enableTimeToLive(ttlConfig);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public Tuple2<LogEntity, Integer> map(LogEntity logEntity) throws Exception {
        Integer lastInterestVal = state.value();
        int times = 1;
        // 1 -> 浏览  2 -> 分享  3 -> 购物
        int thisActionType = Integer.parseInt(logEntity.getAction());

        if (state.value() == null) {
            state.update(0);
        }

        state.update(state.value() + thisActionType);

        if (logEntity.getAction().equals("3")) {
            state.clear();
        }
        return Tuple2.of(logEntity, times);
    }
}
