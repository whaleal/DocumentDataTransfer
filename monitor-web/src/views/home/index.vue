<template>
  <div class="box-card">
    <el-card shadow="none">
      <el-card shadow="none">
        <div class="card-header">
          <div>
            <!-- <el-card shadow='none'> -->
            <h5 style="text-align:center;">
              <i class="el-icon-document" style="font-size:18px;"></i>
              <a href="https://docs.whaleal.com/guide/" class="help">Documentation</a>
            </h5>
            <!-- </el-card> -->
          </div>
          <div>
            <!-- <el-card shadow='none'> -->
            <h5 style="text-align:center;">
              <i class="el-icon-office-building" style="font-size:18px;"></i>
              <a href="https://www.whaleal.com" class="help">Community Address</a>
            </h5>
            <!-- </el-card> -->
          </div>
        </div>
      </el-card>
      <div>
        <div class="heading text-center">
          <h2>About Whaleal</h2>
          <p class="lead text-center mb-5">
            Whaleal Information is a professional IT data consulting and service provider, committed to delivering high-quality information products, consulting, and services to users.
            <br/>
            Whaleal Information was founded in Shanghai in 2015 and has branches in Beijing, Shenzhen, and Guangzhou.
            <br/>
            Whaleal Information is a core partner of MongoDB in the North Asia region and the core partner of Akamai and Vonage in China.
          </p>
        </div>
        <div class="heading text-center">
          <h2>Partnerships</h2>
          <p class="lead text-center mb-5">
            Whaleal Information has become a service provider for over 50 leading enterprises in the financial, insurance, securities, gaming, and e-commerce industries, including Taiping Insurance, Taikang Insurance, Pacific Insurance, Ctrip, China Eastern Airlines, and Guotai Junan Securities.
          </p>
          <div class="banner">
            <el-carousel height="200px"  direction="vertical" :autoplay="true" :interval="2000" loop >
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
          <h2>Services Provided by Whaleal</h2>
          <p class="lead text-center mb-5">
            Platform Installation and Deployment Services  Platform Training Services
            <br/>
            Platform Consulting Services  SLA Services  Platform Annual Inspection
            <br/>
            Bug Patch Upgrades  Custom Development
          </p>
        </div>
      </div>
      <div class="heading text-center">
        <h2>Future Features</h2>
        <div class="tool-box">
          <div class="row">
            <div class="row-div">
              <el-row>
                <el-col :span="4" :xs="12">
                  <img src="../../assets/index-about-1.png" alt="" class="flex-img">
                </el-col>
                <el-col :span="16" :xs="12">
                  <p class="index-about-nav">
                    <span>Compatibility</span>
                    <br/>
                    Migration of multiple relational databases to MongoDB
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
                    <span>Scalability</span>
                    <br/>
                    Horizontal scaling at the application and storage layers
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
                    <span>Scheduling</span>
                    <br/>
                    Parallel task orchestration and scheduling
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
                    <span>Security</span>
                    <br/>
                    Consistent data rollback for abnormal tasks
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
                    <span>Real-time Monitoring</span>
                    <br/>
                    Real-time monitoring of tasks
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
                    <span>Cold and Hot Data</span>
                    <br/>
                    Integration with HDFS, a distributed file system
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
                    <span>Bidirectionality</span>
                    <br/>
                    Interconversion between relational and non-relational data
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
                    <span>Localization</span>
                    <br/>
                    Compatible with the migration of domestic databases
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
