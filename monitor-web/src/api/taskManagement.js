import { axiosGet, axiosPost } from '../utils/request'
/**
 *@description: 获取所有任务
 */
 export const getWorkList = params =>
 axiosGet({
   method: 'get',
   url: '/api/work/getWorkInfoList',
   params
 })


 /**
 *@description: 获取任务info
 */
 export const getWorkInfo = workName =>
 axiosGet({
   method: 'get',
   url: '/api/work/getWorkInfo/'+workName,
 })

 /**
 *@description: 获取task Echart
 */
 export const getWorkMonitor =(workName,params) =>
 axiosGet({
   method: 'get',
   url: '/api/work/getWorkMonitor/'+workName,
   params
 })

 /**
 *@description: 获取所有日志
 */
 export const getTaskLog =(id,params) =>
 axiosGet({
   method: 'get',
   url: '/api/log/getLog/'+id,
   params
 })



