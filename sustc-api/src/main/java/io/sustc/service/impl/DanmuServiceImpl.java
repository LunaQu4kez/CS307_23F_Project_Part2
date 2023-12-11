package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;

import java.util.List;

public class DanmuServiceImpl implements DanmuService {
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        return 0;
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        return null;
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        return false;
    }
}
