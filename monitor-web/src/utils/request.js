import axios from 'axios'
import { Message } from 'element-ui';
import route from '../router/index';


// const constBaseURL = 'http://whalealDDT.com:58001';
const constBaseURL = 'http://192.168.12.190:58001';
export default constBaseURL;

// // post请求头
axios.defaults.headers.post['Content-Type'] = 'application/json'

export function axiosGet(config) {
    // 创建axios
    const axiosGet = axios.create({
        baseURL: constBaseURL,
    });

    axiosGet.interceptors.request.use(
        config => {
            config.headers.token = localStorage.getItem("token");
            return config;
        },
        error => {
            // 先输出到控制台上 方便用户查看
            console.log(error);
            // 不可把错误信息展示给前端用户
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        }
    );

    axiosGet.interceptors.response.use(
        response => {
            // response.status是前端游览器自带的状态标识符 常见 200 404 500等
            if (response.status === 200) {
                // response.data.code为后端返回的数据
                if(response.data.code===1000){
                    // code===1000 正常执行
                    return response.data;
                }
                else if (response.data.code === 1107) {
                    // code===1107 未登录或token失效
                    localStorage.removeItem("token");
                    Message.warning("请先登录");
                    route.replace({path:'/login'})
                }
                else if(response.data.code > 1000){
                    // code>1000的msg都要进行展示
                    Message.error(response.data.msg);
                }else if(response.data.code < 1000){
                    // code<1000的msg 均不展示到前端
                     console.log(response.data.msg);
                }
                return  Promise.reject(response.data);
            } else {
                console.log(response);
                Message.error ({message:'请求错误!'});
                return  Promise.reject(response);
            }
        },
        error => {
            console.log(error);
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        });
    return axiosGet(config);
}

export function axiosPost(config) {
    const axiosPost = axios.create({
        baseURL: constBaseURL,
      timeout: 10000
    })

    axiosPost.interceptors.request.use(
      config => {
        config.headers.token = localStorage.getItem("token");
        return config
      },
      error => {
        // 先输出到控制台上 方便用户查看
        console.log(error)
        // 不可把错误信息展示给前端用户
        Message.error({ message: '请求超时!' })
        return Promise.reject(error)
      }
    )

    axiosPost.interceptors.response.use(
      response => {
        // response.status是前端游览器自带的状态标识符 常见 200 404 500等
        if (response.status === 200) {
          // response.data.code为后端返回的数据
          if (response.data.code === 1000) {
            // code===0 正常执行
            return response.data
          } else if (response.data.code === 1107) {
            // code===1107 未登录或token失效
            localStorage.removeItem('token')
            Message.warning('请先登录')
            route.replace({ path: '/login' })
          } else if (response.data.code > 1000) {
            // code>1000的msg都要进行展示
            Message.error(response.data.msg)
          } else if (response.data.code < 1000) {
            // code<1000的msg 均不展示到前端
            console.log(response.data.msg)
          }
          return Promise.reject(response.data)
        } else {
          console.log(response)
          Message.error({ message: '请求错误!' })
          return Promise.reject(response)
        }
      },
      error => {
        console.log(error)
        Message.error({ message: '请求超时!' })
        return Promise.reject(error)
      }
    )
    return axiosPost(config)
  }

export function axiosDelete(url ,data= {}) {
    const axiosDelete = axios.create({
        baseURL: constBaseURL,
    });

    axiosDelete.interceptors.request.use(
        config => {
            config.headers.token = localStorage.getItem("token");
            return config;
        },
        error => {
            // 先输出到控制台上 方便用户查看
            console.log(error);
            // 不可把错误信息展示给前端用户
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        }
    );

    axiosDelete.interceptors.response.use(
        response => {
            // response.status是前端游览器自带的状态标识符 常见 200 404 500等
            if (response.status === 200) {
                // response.data.code为后端返回的数据
                if(response.data.code===1000){
                    // code===1000 正常执行
                    return response.data;
                }
                else if (response.data.code === 1107) {
                    // code===1107 未登录或token失效
                    localStorage.removeItem("token");
                    Message.warning("请先登录");
                    route.replace({path:'/login'})
                }
                else if(response.data.code > 1000){
                    // code>1000的msg都要进行展示
                    Message.error(response.data.msg);
                }else if(response.data.code < 1000){
                    // code<1000的msg 均不展示到前端
                    console.log(response.data.msg);
                }
                return  Promise.reject(response.data);
            } else {
                console.log(response);
                Message.error ({message:'请求错误!'});
                return  Promise.reject(response);
            }
        },
        error => {
            console.log(error);
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        });
    return axiosDelete.delete(url, data);
}

export function axiosPut(url, data = {}) {
    const axiosPut = axios.create({
        baseURL: constBaseURL,
    });


    axiosPut.interceptors.request.use(
        config => {
            config.headers.token = localStorage.getItem("token");
            return config;
        },
        error => {
            // 先输出到控制台上 方便用户查看
            console.log(error);
            // 不可把错误信息展示给前端用户
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        }
    );

    axiosPut.interceptors.response.use(
        response => {
            // response.status是前端游览器自带的状态标识符 常见 200 404 500等
            if (response.status === 200) {
                // response.data.code为后端返回的数据
                if(response.data.code===1000){
                    // code===1000 正常执行
                    return response.data;
                }
                else if (response.data.code === 1107) {
                    // code===1107 未登录或token失效
                    localStorage.removeItem("token");
                    Message.warning("请先登录");
                    route.replace({path:'/login'})
                }
                else if(response.data.code > 1000){
                    // code>1000的msg都要进行展示
                    Message.error(response.data.msg);
                }else if(response.data.code < 1000){
                    // code<1000的msg 均不展示到前端
                    console.log(response.data.msg);
                }
                return  Promise.reject(response.data);
            } else {
                console.log(response);
                Message.error ({message:'请求错误!'});
                return  Promise.reject(response);
            }
        },
        error => {
            console.log(error);
            Message.error ({message:'请求超时!'});
            return Promise.reject(error);
        });
    return axiosPut.put(url, data);
}
