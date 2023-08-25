import { axiosGet, axiosPost } from '../utils/request'

 export const findLog = params =>
 axiosGet({
   method: 'get',
   url: '/api/log/findLog',
   params
 })




