<template>
    <div class="pagination-box" v-if="total !== 0">
      <el-pagination
        :current-page="currentPage"
        :page-size="pageSize"
        :page-sizes="[5, 10, 20, 30]"
        background
        layout="total, sizes, prev, pager, next, jumper"
        :total="total"
        @size-change="sizeChange"
        @current-change="currentChange"
      />
    </div>
  </template>
  
  <script >
    export default{
        data(){
            return{
            
            }
        },
        props:{
            total:{
                type:Number,
                default:0
            },
            currentPage:{
                type:Number,
                default:1
            },
            pageSize:{
                type:Number,
                default:5
            },
        },

        mounted(){
        
        },

        methods:{

            sizeChange(val){
                this.$emit('handleSizeChange', val)
            },

            currentChange(val){
                this.$emit('handleCurrentChange', val)
            }
        }
    }

  </script>
  
  <style scoped>
  .pagination-box {
    margin-top: 20px;
    
  }
  .el-pagination{
    text-align: center;
  }
  </style>
  