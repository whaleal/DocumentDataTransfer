package com.whaleal.ddt.monitor.service;


import java.util.List;
import java.util.Map;

public interface WorkService {


    void upsertWorkInfo(String workName, Map<Object, Object> workInfo);

    Map<Object, Object>  getWorkInfo(String workName);
}
