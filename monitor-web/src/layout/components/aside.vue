<template>
  <div style="background-color:#304156">
    <el-menu class="el-menu-vertical-demo" :collapse="isCollapse" :default-active="$route.path" :unique-opened='true'
             router
             background-color="#304156"
             text-color="rgb(191, 203, 217)"
             active-text-color="rgb(64, 158, 255)"
    >
      <template v-for="item in items">
        <template v-if="item.subs">
          <el-submenu :index="item.index" :key="item.index">
            <template slot="title">
              <i :class="item.icon">
              </i>
              <span slot="title">{{ item.title }}</span>
            </template>
            <template v-for="subItem in item.subs">
              <el-submenu
                  v-if="subItem.subs"
                  :index="subItem.index"
                  :key="subItem.index"
              >
                <template slot="title">{{ subItem.title }}</template>
                <el-menu-item
                    v-for="(threeItem,i) in subItem.subs"
                    :key="i"
                    :index="threeItem.index"
                >{{ threeItem.title }}
                </el-menu-item>
              </el-submenu>
              <el-menu-item
                  v-else
                  :index="subItem.index"
                  :key="subItem.index"
              >
                <template slot="title">
                                <span>
                                    {{ subItem.title }}
                                </span>
                </template>
              </el-menu-item>
            </template>
          </el-submenu>
        </template>
        <template v-else>
          <el-menu-item :index="item.index" :key="item.index">
            <i v-if="item.title=='MongoList'" class="el-icon-user"></i>
            <i v-else :class="item.icon"></i>
            <span slot="title">{{ item.title }}</span>
          </el-menu-item>
        </template>
      </template>
    </el-menu>

  </div>
</template>
<script>
export default {
  data() {
    let role = localStorage.getItem('role');
    return {
      role: '',
      items: [
        {
          icon: 'el-icon-menu',
          index: `/home`,
          title: '首页'
        },

        {
          icon: 'el-icon-date',
          index: '/taskManagement',
          title: '任务管理',
        },
        {
          icon: 'el-icon-notebook-2',
          index: '/log',
          title: '日志列表',
        },

      ],
      createMongoDBAble: '',

    }
  },
  props: {
    isCollapse: {
      type: Boolean,
      default: true
    }
  },
  created() {
    this.role = localStorage.getItem('role');
  },
  mounted() {
    this.roleSplice(this.role)
    this.createMongoDBAble = JSON.parse(localStorage.getItem('createMongoDBAble'))
    this.admin()
  },
  computed: {
    onRoutes() {
      return this.$route.path.replace('/', '');
    }
  },
  methods: {
    //判断是否是管理员admin
    admin() {
      if (this.createMongoDBAble) {
        return this.items
      } else {
        this.items.forEach((items, index, val) => {
          //    if(items.title=='MongoDB'){
          //         items.subs.forEach((item,index)=>{
          //             if(item.title =='MongoTars'){
          //                 // console.log(items);
          //                 items.subs.splice(index,1)
          //             }
          //         })
          //    }
        });
        return this.items
      }
    },

    //判断role是否为admin
    roleSplice(role) {
      if (role == 'admin') {
        return this.items
      } else {
        this.items.forEach((items, index, val) => {
          if (items.title == 'Project' || items.title == 'Settings') {
            this.items.splice(index, 1)
          }
        });
        return this.items
      }
    }
  }
}
</script>
<style scoped>
.el-menu-vertical-demo:not(.el-menu--collapse) {
  width: 200px;
  /* min-height: 400px; */
}


.help {
  font-size: 14px;
  text-decoration: none;
  color: rgb(0, 124, 173);
}
</style>
