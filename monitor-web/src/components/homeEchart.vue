<template>
    <div>
        <div class="home_echart_title">
            <div>
                {{name}}
            </div>
            <div >
            <el-popover
                placement="bottom-start"
                width="150"
                trigger="hover"
            >
                <span>{{message}}</span>
                <v-mdi name='mdi-helpCircleOutline' class="home_help-tips" slot="reference"></v-mdi>

            </el-popover>
            </div>
        </div>

        <div  :id="name" :ref="name" style="height:200px;" shadow="none"  >

        </div>  
    </div>
</template>

<script>

export default {
    props:{
        name:{
            type:String,
            default:()=>{}
        },
        drawDataList:{
            type:Array,
            default:()=>{}
        },
        type:{
            type:String,
            default:()=>{}
        },
        total:{
            type:Boolean,
            default:true
        }
        
    },
    data(){
        return{
            drawData:{},
            series:[],
            xAxis:[],
            message:'',
            color:'',
            unit:''
        }
    },
    mounted(){

    },

    watch:{
        drawDataList:{
            handler(newVal,oldVal){
                for(let a=0;a<this.drawDataList.length;a++){
                    if(this.name == this.drawDataList[a].drawName){
                        this.drawData = this.drawDataList[a].drawDataList
                        this.name = this.drawDataList[a].drawName
                        this.xAxis = this.drawDataList[a].drawCreateTime
                        this.message = this.drawDataList[a].message,
                        this.unit = this.drawDataList[a].unit
                    }
                }
                this.$nextTick(()=>{
                    this.drawer()
                })
            },
            deep:true,
            immediate:true
        },
        
    },

    methods:{
        drawer(){
            // for(let l = 0;l<Object.keys(this.monitorList).length;l++){
                // let myChart=''
                this.series=[];
                let that = this
                // console.log(this.monitorList);
                for (let key in this.drawData){
                    if(this.type=='line'){
                        that.series.push(
                            {
                                name: key,
                                type: this.type,
                                smooth:true,
                                stack:'Total',
                                symbol:'none',
                                areaStyle: {
                                    opacity:this.total?0.7:0,
                                    // color: new this.$echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                    //     {
                                    //         offset: 0,
                                    //         color: 'rgb(0, 221, 255)'
                                    //     },
                                    //     {
                                    //         offset: 1,
                                    //         color: 'rgb(77, 119, 255)'
                                    //     },
                                        
                                    // ]),
                                    global:false
                                },
                                emphasis: {
                                    focus: 'series'
                                },
                                data: this.drawData[key],
                                itemStyle:{
                                    normal:{
        
                                        // color:this.generateColor()
                                        // lineStyle:{
                                        //     color:function(params){
                                        //         console.log(params);
                                        //         var color= '#'
                                        //         for(var a = 0;a<6;a++){
                                        //             color+=Math.floor(Math.random()*9)
                                        //         }   

                                        //         // console.log(color);
                                                    
                                        //             return color
                                        //     }
                                        // }
                                    }
                                },
                                
                            }
                        )       
                    }
                    else if(this.type=='pie'){
                        that.series.push(
                            {
                                value: this.drawData[key],
                                name:key
                            }
                        )       
                    }

                }

                let data = that.series
                  setTimeout(()=>{
                        if(this.type=='line'){
                            let option={
                                title:{
                                    // text:this.name,
                                    textStyle:{

                                    }
                                },

                                grid:{
                                    // x:'12%',
                                    y:'25%',
                                    y2:'10%',
                                    left:'15%'
                                    // height:'90%'
                                },

                                legend:{
                                    show:true
                                },
                                dataZoom:[
                                    {
                                        // type: 'inside',
                                        show:false,
                                        xAxisIndex: 0,
                                        zoomOnMouseWheel: false //鼠标滚轮不能触发缩放
                                    }
                                ],
                                toolbox:{
                                    feature: {
                                        // dataView: { show: true, readOnly: false },
                                        // magicType: { show: true, type: ['line', 'bar'] },
                                        // restore: { show: true },
                                        // saveAsImage: { show: true }
                                        dataZoom:{
                                            show:false,
                                            filterMode:'filter',
                                            title: {
                                                zoom: '缩放',
                                                back: '还原'
                                            },
                                            iconStyle:{ //不需要图标可以设置隐藏按钮
                                            opacity:0
                                            },
                                            //缩放和还原的图标路径，不指定则显示默认图标
                                            // icon: {
                                            //     zoom: '',
                                            //     back: ''
                                            // },
                                            // xAxisIndex: true, //指定哪些X轴可以被控制
                                            yAxisIndex:'none' //指定哪些Y轴可以被控制（设置为 false，则不控制任何y轴）
                                        },
                                        // restore: {} //区域缩放重置
                                    },
                                },
                                tooltip:{
                                    trigger:'axis',
                                    axisPointer:{
                                        type:'cross',
                                        snap:true,
                                        lineStyle:{
                                            type:'solid',
                                            width:2,
                                            backgroundColor:'#6a7985'
                                        }
                                    },

                                    confine:true,

                                },
                                
                                xAxis:{
                                    type:'category',
                                    data:this.$dateListZoneFtt(this.xAxis),
                                    // offset:10,
                                    boundaryGap:false,
                                    axisLabel:{
                                        // interval: this.$dateListZoneFtt(this.xAxis) > 15 ? 30 : 0,   //主要设置其间隔，间隔为3
                                    }
                                },

                                yAxis:{
                                    type:'value',
                                    name:this.unit,
                                    minInterval: 1,
                                    nameTextStyle: {
                                        color: "#aaa",
                                        nameLocation: "start",
                                    },   
                                },
                                    series: data
                            }
                            let myCharts = document.getElementById(this.name);
                            let myChart = this.$echarts.init(myCharts);
                            myChart.setOption(option,true)
                            myChart.dispatchAction({
                                // 默认选中区域缩放
                                type: 'takeGlobalCursor',
                                key: 'dataZoomSelect',
                                dataZoomSelectActive: true
                            });
                            myChart.group = this.name;
                            this.$echarts.connect(this.name);
                            // myChart = '';
                            that.series=[];
                            window.addEventListener('resize',()=>{
                                myChart && myChart.resize()
                            })
                        } else if(this.type =='pie'){
                            let option ={
                                title:{
                                    // text:this.name
                                },
                                grid:{
                                    // x:'12%',
                                    y:'25%',
                                    y2:'10%',
                                    // left:'15%'
                                    // height:'90%'
                                },
                                tooltip:{
                                    trigger:'item',
                                },
                            
                                label:{
                                    normal:{
                                        show:true,
                                        // position:'inner',
                                        formatter:'{b} {c} '
                                    }
                                },
                                emphasis: {
                                    itemStyle: {
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                                    }
                                },
                                series:[
                                    {
                                        type:this.type,
                                        radius:'70%',
                                        data:data,
                                        label:{
                                            normal:{
                                                formatter:'{b}: {c} ({d}%)'
                                            }
                                        }
                                    }
                                ],

                            }
                            let myCharts = document.getElementById(this.name);
                            let myChart = this.$echarts.init(myCharts);
                            myChart.setOption(option,true)
                            myChart.dispatchAction({
                                // 默认选中区域缩放
                                type: 'takeGlobalCursor',
                                key: 'dataZoomSelect',
                                dataZoomSelectActive: true
                            });
                            myChart.group = this.name;
                            this.$echarts.connect(this.name);
                            // myChart = '';
                            that.series=[];
                            window.addEventListener('resize',()=>{
                                myChart && myChart.resize()
                            })
                        }
                },0)

               

            // }


       },
    }

}
</script>

<style>
    .home_echart_title{
        display: flex;
        font-weight:bolder;
        color: #888;
    }

    .home_help-tips{
        margin-left: 5px;
        width:20px;
        height:20px;
    }
</style>