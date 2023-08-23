<template>
    <div class="box-card">
        <el-card shadow="none">
            <el-card shadow="none">
                <div class="card-header">
                        <div>
                            <!-- <el-card shadow='none'> -->
                                <h5 style="text-align:center;">
                                    <i class="el-icon-document" style="font-size:18px;"></i>
                                    <a href="https://docs.whaleal.com/guide/" class="help">文档专栏</a>
                                </h5>
                            <!-- </el-card> -->
                        </div>
                        <div>
                            <!-- <el-card shadow='none'> -->
                                <h5 style="text-align:center;">
                                    <i class="el-icon-office-building" style="font-size:18px;"></i>
                                    <a href="https://www.whaleal.com" class="help">社区地址</a>
                                </h5>
                            <!-- </el-card> -->
                        </div>
                </div>
            </el-card>
            <div>
                <div class="heading text-center">
                    <h2>关于锦木</h2>
                    <p class="lead text-center mb-5">
                        锦木信息是专业的IT数据咨询与服务提供商，致力于向用户提供优质的信息化产品、咨询及服务。
                        <br/>
                        锦木信息于2015年在上海成立，并于北京、深圳、广州设立分支机构。
                        <br/>
                        锦木信息是<span>MongoDB北亚区核心合作伙伴，Akamai、Vonage的中国区核心合作伙伴</span>。
                    </p>
                </div>
                <div class="heading text-center">
                    <h2>合作伙伴</h2>
                     <p class="lead text-center mb-5">
                        锦木信息已成为太平保险、泰康保险、太平洋保险、携程、中国东方航空、国泰君安证券等50+金融、保险、券商、游戏、电商行业龙头企业服务商。
                    </p>
                    <div class="banner">
                        <el-carousel height="200px" direction="vertical" :autoplay="true" :interval="1000" loop >
                            <el-carousel-item>
                                <ul class="banner_img">
                                    <li  v-for="items in img_list" :key="items" >
                                        <img :src="items" alt='' >
                                    </li>
                                </ul>
                            </el-carousel-item>
                            <el-carousel-item>
                                <ul class="banner_img">
                                    <li  v-for="items in img_list2" :key="items" >
                                        <img :src="items" alt='' >
                                    </li>
                                </ul>
                            </el-carousel-item>
                             <el-carousel-item>
                                <ul class="banner_img">
                                    <li v-for="items in img_list3" :key="items">
                                        <img :src="items" alt='' >
                                    </li>
                                </ul>
                            </el-carousel-item>
                        </el-carousel>
                    </div>
                </div>
                <div class="heading text-center">
                    <h2>锦木提供</h2>
                    <p class="lead text-center mb-5">
                        平台安装部署服务  平台培训服务
                        <br/>
                        平台咨询服务  SLA服务  平台年度巡检
                        <br/>
                        Bug补丁升级  定制化开发
                    </p>
                </div>
            </div>
             <div class="heading text-center">
                <h2>功能展望</h2>
                <div class="tool-box">
                    <div class="row">
                            <div class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>兼容性</span>
                                            <br/>
                                            多关系型数据库迁移至MongoDB
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>拓展性</span>
                                            <br/>
                                            应用层和存储层水平拓展
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>调度性</span>
                                            <br/>
                                            对任务串并行编排调度
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>安全性</span>
                                            <br/>
                                            异常任务回滚数据一致
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>

                    </div>
                    <div class="row">
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>实时性</span>
                                            <br/>
                                            任务的实时监控
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>冷热数据</span>
                                            <br/>
                                            集成HDFS，分布式文件系统
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>

                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>双向性</span>
                                            <br/>
                                            关系型和非关系性互相转换
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                            <div  class="row-div">
                                <el-row>
                                    <el-col :span="4" :xs="12">
                                        <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                                    </el-col>
                                    <el-col :span="16" :xs="12">
                                        <p class="index-about-nav">
                                            <span>国产化</span>
                                            <br/>
                                            兼容国产数据库迁移
                                        </p>
                                    </el-col>
                                </el-row>
                            </div>
                    </div>
                </div>
            </div>
        </el-card>
    </div>
</template>

<script>

export default {
    name:'support',

    data(){
        var img1 = new URL("../../assets/1.png",import.meta.url).href
        var img2 = new URL("../../assets/2.png",import.meta.url).href
        var img3 = new URL("../../assets/3.png",import.meta.url).href
        var img4 = new URL("../../assets/4.png",import.meta.url).href
        var img5 = new URL("../../assets/5.png",import.meta.url).href
        var img6 = new URL("../../assets/6.png",import.meta.url).href
        var img7 = new URL("../../assets/7.png",import.meta.url).href
        var img8 = new URL("../../assets/8.png",import.meta.url).href
        var img9 = new URL("../../assets/9.png",import.meta.url).href
        var img10 = new URL("../../assets/10.jpeg",import.meta.url).href
        var img11 = new URL("../../assets/11.png",import.meta.url).href
        var img12 = new URL("../../assets/12.png",import.meta.url).href
        var img13 = new URL("../../assets/13.png",import.meta.url).href
        var img14 = new URL("../../assets/14.png",import.meta.url).href
        return{
            img_list:[img1,img2,img3,img4,img5,img6],
            img_list2:[img7,img8,img9,img10,img11,img12],
            img_list3:[img10,img3,img5,img13,img14,img1]
        }
    },

    methods:{

    }

}
</script>

<style >
    .box-card{
        margin: 10px;
    }

    .card-header{
        display: flex;
        justify-content: space-around;
    }

    .heading{
        margin-top: 15px;
        margin-bottom: 0.8rem;
    }

    .text-center{
        text-align: center;
    }

    .heading h2::after{
        content: " ";
        display: block;
        width: 100px;
        height: 7px;
        margin: 0 auto;
        margin-top: 0.5rem;
        background: url('../../assets/index-t-b.png') no-repeat center bottom;
    }

    .lead{
        /* margin-top: 10px; */
        font-size: 0.9rem;
        font-weight: 500;
        letter-spacing: 0rem;
        line-height: 2rem;
    }

    .lead span{
        color: #024d70;
    }

    .flex-img{
        max-width: 100%;
        vertical-align: middle;
        border-style: none;
    }
    .tool-box{
        margin: 20px 40px 0;;
    }

    .row{
        display: flex;
        justify-content: space-evenly;
        /* flex: 1; */
    }

    .row-div{
        width:20%;
    }

    .index-about-nav{
        width: 95%;
        line-height: 1.5rem;
        text-align: justify;
        color: #555;
    }

    .index-about-nav span{
        font-weight: 700;
        font-size: 1rem;
        color:#333;
    }

    .banner{
        padding: 0 10% 0;
    }

    @media only screen and (max-width: 800px) {
        .banner {
            padding: 0;
        }
    }


    .banner_img{
        display: flex;
        flex-direction: row;
        margin-bottom: -14px;
        align-content: space-between;
        flex-wrap: wrap;

    }

    .banner_img>li{
        /* text-align: center; */
        /* flex: 1; */
        margin: 0 16px;
        width: 30%;
    }


    .banner_img>li>img{
        margin: 0 auto;
    }

    .el-carousel__item h3 {
        /* color: #475669; */
        font-size: 14px;
        opacity: 0.75;
        line-height: 200px;
        margin: 0;
    }

    .help{

        font-size: 18px;
        text-decoration: none;
        margin-left: 5px;
        color: rgb(0, 124, 173);

    }



    .el-carousel__item:nth-child(2n) {
        /* background-color: #99a9bf; */
    }

    .el-carousel__item:nth-child(2n+1) {
        /* background-color: #d3dce6; */
    }

</style>
