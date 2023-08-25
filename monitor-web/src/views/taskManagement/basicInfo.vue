<template>
  <div>
    <el-descriptions :title="workName" :column="1" size="medium" border>
      <el-descriptions-item label="Hostname">{{ info.config.hostName }}</el-descriptions-item>
      <el-descriptions-item label="Process ID">
        <el-tag size="small" style="margin-right:10px;">{{ info.config.pid }}</el-tag>
      </el-descriptions-item>
      <el-descriptions-item label="Process Directory">{{ info.config.bootDirectory }}</el-descriptions-item>
      <el-descriptions-item label="Current Sync Mode">{{ info.config.syncMode }}</el-descriptions-item>
      <el-descriptions-item label="Start Time">{{ this.$dateZoneFtt(info.config.startTime, null) }}</el-descriptions-item>
      <el-descriptions-item label="End Time">{{ this.$dateZoneFtt(info.config.endTime, null) }}</el-descriptions-item>
      <el-descriptions-item label="JVMArg">{{ info.config.JVMArg }}</el-descriptions-item>
    </el-descriptions>

    <div style="margin-top:10px;">
      <el-collapse v-model="activeName" accordion>
        <el-collapse-item title="Configuration" name="1">
          <el-form
              :model="info.config"
              ref="createOrEditFormRef"
              label-position="right"
              label-width="130px">

            <el-row>
              <el-col span="12">
                <el-form-item label="Work Name">
                  <el-input v-model="info.config.workName" size="small" disabled ></el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-row>
              <el-col>
                <el-form-item label="Source URL" >
                  <el-input v-model="info.config.sourceDsUrl" size="small" style="width:100%" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-row>
              <el-col>
                <el-form-item label="Target URL" >
                  <el-input v-model="info.config.targetDsUrl" size="small" style="width:100%" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>


            <el-row>
              <el-col :span="10">
                <el-form-item label="Sync Mode" >
                  <el-select v-model="info.config.syncMode" size="small" disabled>
                    <el-option
                        v-for="item in syncMode"
                        :key="item.value"
                        :label="item.label"
                        :value="item.value"/>
                  </el-select>
                </el-form-item>

              </el-col>
              <el-col :span="10">
                <el-form-item label="Sync Method" label-width="100px">
                  <el-input v-if="info.config.syncMode==='all'" v-model="info.config.fullType" size="small"
                            style="width:100%" disabled></el-input>
                  <el-input v-if="info.config.syncMode==='realTime'" v-model="info.config.realTimeType" size="small"
                            style="width:100%" disabled></el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-row>
              <el-col :span="12">
                <el-form-item label="Sync Table List" >
                  <el-input v-model="info.config.dbTableWhite" size="small" style="width:100%" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-form-item label="Sync DDL"  v-if="info.config.syncMode==='realTime'">
              <el-select v-model="info.config.ddlFilterSet" multiple size="small" style="width:100%" disabled>
                <el-option
                    v-for="item in ddlFilterSet"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value"/>
              </el-select>
            </el-form-item>

            <el-form-item label="Pre-process" prop="clusterDDL" v-if="info.config.syncMode==='all'">

              <el-select v-model="info.config.clusterInfoSet" multiple clearable size="small" style="width:100%"
                         disabled>
                <el-option
                    v-for="item in clusterDDL"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value"/>
              </el-select>
            </el-form-item>

            <el-row v-if="info.config.syncMode==='all'">
              <el-col :span="6">
                <el-form-item label="Read Threads" >
                  <el-input v-model.number="info.config.sourceThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>

              <el-col :span="6">
                <el-form-item label="Write Threads" >
                  <el-input v-model.number="info.config.targetThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>


              <el-col :span="10">
                <el-form-item label="Index Threads" >
                  <el-input v-model.number="info.config.createIndexThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>


            <el-row>
              <el-col :span="5">
                <el-form-item label="Bucket Size">
                  <el-input v-model.number="info.config.bucketSize" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
              <el-col :span="5">
                <el-form-item label="Bucket Count">
                  <el-input v-model.number="info.config.bucketNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
              <el-col :span="5">
                <el-form-item label="Batch Data Size">
                  <el-input v-model.number="info.config.batchSize" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>

            <el-row v-if="info.config.syncMode==='realTime'">
              <el-col :span="8">
                <el-form-item label="Oplog Start Time" >
                  <el-date-picker
                      v-model="info.config.startOplogTime * 1000"
                      type="datetime"
                      value-format="timestamp"
                      placeholder="Select Date Time"
                      size="small"
                      disabled>
                  </el-date-picker>
                </el-form-item>
              </el-col>

              <el-col :span="8" v-if="info.config.endOplogTime>0">
                <el-form-item label="Oplog End Time" >
                  <el-date-picker
                      v-model="info.config.endOplogTime* 1000"
                      type="datetime"
                      value-format="timestamp"
                      placeholder="Select Date Time"
                      size="small"
                      disabled
                  >
                  </el-date-picker>
                </el-form-item>
              </el-col>
              <el-col :span="6">
                <el-form-item label="Oplog Delay Time">
                  <el-input v-model.number="info.config.delayTime" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>


            <el-row v-if="info.config.syncMode==='realTime'">
              <el-col :span="10">
                <el-form-item label="Bucket Threads" >
                  <el-input v-model.number="info.config.nsBucketThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>

              </el-col>

              <el-col :span="10">
                <el-form-item label="Write Threads">
                  <el-input v-model.number="info.config.writeThreadNum" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>

            </el-row>


            <el-row>

              <el-col :span="5">
                <el-form-item label="Max DDL Time" >
                  <el-input v-model.number="info.config.ddlWait" size="small" disabled>
                  </el-input>
                </el-form-item>
              </el-col>
            </el-row>

          </el-form>

        </el-collapse-item>

      </el-collapse>
    </div>
  </div>
</template>

<script>
import { getWorkInfo } from '@/api/taskManagement';

export default {
  props: {
    workName: {
      type: String,
      default: null,
    },
  },
  data() {
    return {
      info: {
        config: {}
      },
      syncMode: [
        {
          label: 'Full',
          value: 'all'
        },
        {
          label: 'Full and Real-time',
          value: 'allAndRealTime'
        },
        {
          label: 'Full and Incremental',
          value: 'allAndIncrement'
        },
        {
          label: 'Real-time',
          value: 'realTime'
        },
      ],
      ddlFilterSet: [],
      clusterDDL: [
        {
          label: 'Delete Existing Tables',
          value: '0'
        },
        {
          label: 'Print All User Information in the Cluster',
          value: '1'
        },
        {
          label: 'Sync Table Structure in the Cluster',
          value: '2'
        },
        {
          label: 'Sync Index Information in the Cluster',
          value: '3'
        },
        {
          label: 'Enable Sharding for All Databases',
          value: '4'
        },
        {
          label: 'Sync Shard Key for Tables',
          value: '5'
        },
        {
          label: 'Sync config.setting Table',
          value: '6'
        },
        {
          label: 'Pre-split Chunks for Databases and Tables',
          value: '7',
        },
      ],
      activeName: '1',
    }
  },

  mounted() {
    this.getWorkInfo()
  },

  methods: {
    getWorkInfo() {
      // Get info
      getWorkInfo(this.workName).then(res => {
        console.log(res);
        this.info.config = res.data
      })
    },

  }
}
</script>
