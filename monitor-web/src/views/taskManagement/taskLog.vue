<!--<template>-->
<!--    <div>-->
<!--        <el-form :inline="true" :model="query" class="demo-form-inline">-->
<!--            <el-row>-->
<!--                <el-col :span="5">-->
<!--                    <el-form-item label="内容">-->
<!--                    <el-input size="small" clearable v-model="query.info" placeholder="请输入内容"></el-input>-->
<!--                    </el-form-item>-->
<!--                </el-col>-->

<!--                <el-col :span="5">-->
<!--                    <el-form-item label="类型">-->
<!--                    <el-select clearable v-model="query.type" placeholder="请选择">-->
<!--                        <el-option-->
<!--                            v-for="item in queryTypeOptions"-->
<!--                            :key="item.value"-->
<!--                            :label="item.label"-->
<!--                            :value="item.value">-->
<!--                        </el-option>-->
<!--                    </el-select>-->
<!--                    </el-form-item>-->
<!--                </el-col>-->

<!--                <el-col :span="10">-->
<!--                    <el-form-item label="日期">-->
<!--                    <el-date-picker-->
<!--                        :picker-options="pickerOptions"-->
<!--                        v-model="dateTimeRange"-->
<!--                        type="datetimerange"-->
<!--                        range-separator="至"-->
<!--                        start-placeholder="开始日期"-->
<!--                        end-placeholder="结束日期">-->
<!--                    </el-date-picker>-->
<!--                    </el-form-item>-->
<!--                </el-col>-->
<!--                <el-button class="el-icon-search" type="primary" @click="searchTaskLog"></el-button>-->
<!--            </el-row>-->
<!--        </el-form>-->

<!--        <div>-->
<!--            <el-table-->
<!--                v-loading="loading"-->
<!--                :data="logList"-->
<!--                style="width: 100%">-->
<!--                <el-table-column-->
<!--                    prop="createTime"-->
<!--                    label="日期"-->
<!--                    sortable-->
<!--                    width="180">-->


<!--                    <template slot-scope="scope">-->
<!--                    {{ $dateZoneFtt(scope.row.time) }}-->
<!--                    </template>-->
<!--                </el-table-column>-->

<!--                <el-table-column-->
<!--                    prop="type"-->
<!--                    label="类型"-->
<!--                    width="180">-->

<!--                    <template slot-scope="scope">-->
<!--                    <el-button size="mini" v-if="scope.row.type==='INFO'" type="success" plain>{{ scope.row.type }}</el-button>-->
<!--                    <el-button size="mini" v-else-if="scope.row.type==='WARN'" type="warning" plain>{{ scope.row.type }}</el-button>-->
<!--                    <el-button size="mini" v-else-if="scope.row.type==='ERROR'" type="danger" plain>{{ scope.row.type }}</el-button>-->
<!--                    <el-button size="mini" v-else-if="scope.row.type==='TRACE'" type="primary" plain>{{ scope.row.type }}</el-button>-->
<!--                    <el-button  v-else size="mini" type="primary" plain>{{scope.row.type}}</el-button>-->
<!--                    </template>-->

<!--                </el-table-column>-->
<!--                <el-table-column-->
<!--                    prop="info"-->
<!--                    label="内容">-->
<!--                </el-table-column>-->
<!--            </el-table>-->
<!--        </div>-->

<!--        <div>-->
<!--             &lt;!&ndash;分页&ndash;&gt;-->
<!--            <my-pagination -->
<!--                :total="total"-->
<!--                :currentPage="query.pageIndex"-->
<!--                :pageSize="query.pageSize"-->
<!--                @handleSizeChange="handleSizeChange"-->
<!--                @handleCurrentChange="handleCurrentChange"-->
<!--            />-->
<!--        </div>-->
<!--    </div>-->
<!--</template>-->

<!--<script>-->
<!--import { getTaskLog,getTaskLogCount } from '@/api/taskManagement';-->
<!--import myPagination from '@/components/myPagination.vue'-->
<!--export default {-->
<!--    components:{-->
<!--        myPagination-->
<!--    },-->
<!--    props:{-->
<!--        processId:{-->
<!--            type:String,-->
<!--            default:()=>{}-->
<!--        }-->
<!--    },-->

<!--    data(){-->
<!--        return{-->
<!--            pickerOptions: {-->
<!--                shortcuts: [-->
<!--                {-->
<!--                    text: '最近一天',-->
<!--                    onClick(picker) {-->
<!--                    const end = new Date();-->
<!--                    const start = new Date();-->
<!--                    start.setTime(start.getTime() - 3600 * 1000 * 24 );-->
<!--                    picker.$emit('pick', [start, end]);-->
<!--                    }-->
<!--                },{-->
<!--                    text: '最近一周',-->
<!--                    onClick(picker) {-->
<!--                    const end = new Date();-->
<!--                    const start = new Date();-->
<!--                    start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);-->
<!--                    picker.$emit('pick', [start, end]);-->
<!--                    }-->
<!--                }, {-->
<!--                    text: '最近一个月',-->
<!--                    onClick(picker) {-->
<!--                    const end = new Date();-->
<!--                    const start = new Date();-->
<!--                    start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);-->
<!--                    picker.$emit('pick', [start, end]);-->
<!--                    }-->
<!--                }]-->
<!--            },-->
<!--            queryTypeOptions: [-->
<!--                {label: "INFO", value: "INFO"}, -->
<!--                {label: "WARN", value: "WARN"}, -->
<!--                {-->
<!--                    label: "TRACE",-->
<!--                    value: "TRACE"-->
<!--                }, -->
<!--                {label: "ERROR", value: "ERROR"},-->
<!--            ],-->
<!--            dateTimeRange: [new Date((new Date().getTime()) - (3600 * 1000)),new Date((new Date().getTime()) + (60 * 1000))],-->
<!--            loading:false,-->
<!--            query:{-->
<!--                type:'',-->
<!--                startTime:'',-->
<!--                endTime:'',-->
<!--                info:'',-->
<!--                pageIndex:1,-->
<!--                pageSize:10,-->
<!--            },-->
<!--            logList:[],-->
<!--            total:0,-->
<!--        }-->
<!--    },-->




<!--    mounted(){-->
<!--        this.init()-->
<!--    },-->

<!--    methods:{-->
<!--        init(){-->
<!--            this.query.startTime = new Date(this.dateTimeRange[0]).getTime(),-->
<!--            this.query.endTime =  new Date(this.dateTimeRange[1]).getTime(),-->
<!--            this.loading = true-->
<!--            getTaskLog(this.processId,this.query).then(res=>{-->
<!--                this.logList = res.data-->
<!--                this.loading = false-->

<!--            });-->

<!--            getTaskLogCount(this.processId,this.query).then(res=>{-->
<!--                this.total = res.count-->
<!--                -->
<!--            })-->
<!--        },-->

<!--        searchTaskLog(){-->
<!--            this.init()-->
<!--        },-->

<!--        // 页面大小改变-->
<!--        handleSizeChange (val) {-->
<!--            this.query.pageSize = val-->
<!--            this.init()-->
<!--        },-->

<!--        // 页面页码改变-->
<!--            handleCurrentChange (val) {-->
<!--            this.query.pageIndex = val-->
<!--            this.init()-->
<!--        },-->

<!--    }-->
<!--}-->
<!--</script>-->
