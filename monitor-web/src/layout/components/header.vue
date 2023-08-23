<template>
	<div class="lay_header">
		<div class="menu-logo">
			<!-- <img src="../../assets/log.png" alt=""> -->
			<div class="header-left">
				<div class="collapse" @click="handleChange">
					<i  v-if='!isCollapse' class="el-icon-s-fold"></i>
					<i v-else class="el-icon-s-unfold"></i>
				</div>
				<navbar />
			</div>
			
		</div>
		 <!-- <div style="text-align:right;"  class="account">
			 <div class="headerBtn">
				<div style="text-align:center;">
				</div>
				 <div class="icon-manner">
				 </div>
				 <el-dropdown  trigger="click" :hide-on-click='false' class="logSelect">
					<span >{{this.userName}}
						<i class="el-icon-arrow-down el-icon--right"></i>
					</span>
					<el-dropdown-menu slot="dropdown" style="width:70px;">
					<el-dropdown-item @click.native="logOut()" style="text-align:center;" >登出</el-dropdown-item>
					</el-dropdown-menu>
				</el-dropdown>
			 </div>

        </div> -->

	</div>
</template>

<script>
import navbar from './navbar.vue'
import bus from '@/common/bus'
export default {
	components:{
		navbar
	},
	data(){
		return{
			userName:'',
			unRead:true,
			memberId:'',
			messageAccount:'',
			timer:'',
			isCollapse:false,
		}
	},
	created(){
		this.userName = localStorage.getItem('memberAccount')
		this.memberId = localStorage.getItem('memberId')
	},

	watch:{

	},

	mounted(){
		// 通过 Event Bus 进行组件间通信，来取消通知红点
		bus.$on('allRead',msg=>{
			this.messageAccount = msg
		})

		bus.$emit('unRead',this.messageAccount);

		// this.getMessageAccount();
		// this.timer = setInterval(this.getMessageAccount,10000);
	},

	updated(){

	},

	beforeDestroy(){
		clearInterval(this.timer)
	},

	methods:{

		messageDetail(){
			this.$router.push(
				{
					path:'/message',
					params:{
						memberId:this.memberId
					}
				}
			);
		},

		toSupport(){
			this.$router.push(
				{
					name:'support',
				}
			)
		},
		
		handleChange(){
            this.isCollapse = !this.isCollapse
			bus.$emit('collapse',this.isCollapse);
			// this.text = this.isCollapse ? '展开' : '收起'
        },

		// getMessageAccount(){
		// 	this.$get({
		// 		methods:'get',
		// 		url:'/server/member/getMessageCount',
		// 		params:{
		// 			startTime:0,
		// 			endTime:0,
		// 			status:false
		// 		}
		// 	}).then(res=>{
		// 		this.messageAccount = res.data
		// 	})
		// },

		logOut(){
		// localStorage.setItem("whaleal-token",'');
		// 		localStorage.removeItem('memberId');
		// 		localStorage.removeItem('memberAccount');
		// 		localStorage.removeItem('member');
		// 		localStorage.removeItem('dataType');
		// 		localStorage.removeItem('memberEmail');
		// 		localStorage.removeItem('memberPhone');
		// 		localStorage.removeItem('task');
				localStorage.clear();
				this.$router.push('/login');


			}
		}
}
</script>

<style >
.lay_header {
	display: flex;
	justify-content: space-between;
	width: 100%;
	height: 60px;
}

.header-left{
	display: flex;
	align-items: center;
	line-height: 60px;
}	

.collapse{
    /* padding-top:15px; */
	margin-left: 10px;
    font-size: 20px;   
	padding-right: 10px; 
    cursor: pointer;
}


	.icon-manner{
		margin: 0 10px;
	}

.headerBtn{
	display: flex;
	margin-top: 20px;
}

.logSelect{
	margin-left: 10px;
}

/* .menu-logo>img{
  display: block;
  width: 170px;
  margin-top: -8px;
  box-sizing: border-box;
  line-height: 60px;
  margin: 0 auto;

} */

.support{
	color: rgb(0, 124, 173);
}




.account:hover{
	cursor: pointer;

}
</style>
