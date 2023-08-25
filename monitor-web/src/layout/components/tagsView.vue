<template>
  <div class="tags-view-container">
    <scroll-pane ref="scrollPane" class="tags-view-wrapper" style="width:92%">
      <div style="float: left;">
        <router-link
            v-for="tag in visitedViews"
            ref="tag"
            :class="isActive(tag)?'active':''"
            :to="{ path: tag.path, query: tag.query, fullPath: tag.fullPath }"
            :key="tag.path"
            tag="span"
            class="tags-view-item"
            @click.middle.native="closeSelectedTag(tag)"
            @contextmenu.prevent.native="openMenu(tag,$event)">
          {{ tag.title }}
          <span v-if="!tag.meta.affix" class="el-icon-close" @click.prevent.stop="closeSelectedTag(tag)"/>
        </router-link>
      </div>
      <el-dropdown style="position: fixed; right: 0; padding: 10px" @command="handleCommand">
  <span class="el-dropdown-link">
    Actions <i class="el-icon-arrow-down el-icon--right"></i>
  </span>
        <el-dropdown-menu slot="dropdown">
          <!-- <el-dropdown-item command="0">Refresh</el-dropdown-item> -->
          <el-dropdown-item command="1">Close</el-dropdown-item>
          <el-dropdown-item command="2">Close Others</el-dropdown-item>
          <el-dropdown-item command="3">Close All</el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>

    </scroll-pane>
  </div>
</template>
<script>
import ScrollPane from '@/components/scrollPane/index.vue'
import path from 'path-browserify'
import {routers} from '@/router'

export default {
  components: {ScrollPane},
  data() {
    return {
      top: 0,
      left: 0,
      affixTags: [],
      selectedTag: {}
    }
  },

  computed: {
    visitedViews() {
      console.log(this.$store.state.tagsView);
      return this.$store.state.tagsView.visitedViews
    }
  },

  watch: {
    $route() {
      this.addViewTags()
      this.moveToCurrentTag()
    },

  },

  mounted() {
    this.addViewTags()
    this.initTags()
  },
  methods: {
    isActive(route) {
      return route.path === this.$route.path
    },

    filterAffixTags(routes, basePath = '/') {
      let tags = []
      routes.forEach(route => {
        if (route.meta && route.meta.affix) {
          const tagPath = path.resolve(basePath, route.path)
          tags.push({
            fullPath: tagPath,
            path: tagPath,
            name: route.name,
            meta: {...route.meta}
          })
        }
        if (route.children) {
          const tempTags = this.filterAffixTags(route.children, route.path)
          if (tempTags.length >= 1) {
            tags = [...tags, ...tempTags]
          }
        }
      })
      return tags
    },

    initTags() {
      const affixTags = this.affixTags = this.filterAffixTags(routers)
      for (const tag of affixTags) {
        // Must have tag name
        if (tag.name) {
          this.$store.dispatch('addVisitedView', tag)
        }
      }
    },

    addViewTags() {
      const {name} = this.$route
      if (name) {
        this.$store.dispatch('addView', this.$route)
        this.selectedTag = this.$route
      }
      return false
    },

    moveToCurrentTag() {
      const tags = this.$refs.tag
      this.$nextTick(() => {
        for (const tag of tags) {
          if (tag.to.path === this.$route.path) {
            this.$refs.scrollPane.moveToTarget(tag)

            // when query is different then update
            if (tag.to.fullPath !== this.$route.fullPath) {
              this.$store.dispatch('updateVisitedView', this.$route)
            }

            break
          }
        }
      })
    },
    refreshSelectedTag(view) {
      this.$store.dispatch('delCachedView', view).then(() => {
        const {fullPath} = view
        // console.log(fullPath);
        // this.$nextTick(() => {
        //     this.$router.replace({
        //         path:fullPath
        //     })
        // })
      })
    },
    closeSelectedTag(view) {
      this.$store.dispatch('delView', view).then(({visitedViews}) => {
        if (this.isActive(view)) {
          const latestView = visitedViews.slice(-1)[0]
          if (latestView) {
            this.$router.push(latestView.path)
          } else {
            this.$router.push('/')
          }
        }
      })
    },

    closeOthersTags() {
      this.$router.push(this.selectedTag)
      this.$store.dispatch('delOthersViews', this.selectedTag).then(() => {
        this.moveToCurrentTag()
      })
    },

    closeAllTags(view) {
      this.$store.dispatch('delAllViews').then(({visitedViews}) => {
        if (this.affixTags.some(tag => tag.path === view.path)) {
          return
        }
        this.toLastView(visitedViews, view)
      })
    },

    toLastView(visitedViews, view) {
      const latestView = visitedViews.slice(-1)[0]
      if (latestView) {
        this.$router.push(latestView)
      } else {
        // now the default is to redirect to the home page if there is no tags-view,
        // you can adjust it according to your needs.
        if (view.name === 'HomePage') {
          // to reload home page
          this.$router.replace({path: '/home'})
        } else {
          this.$router.push('/')
        }
      }
    },

    openMenu(tag, e) {
      const menuMinWidth = 105
      const offsetLeft = this.$el.getBoundingClientRect().left // container margin left
      const offsetWidth = this.$el.offsetWidth // container width
      const maxLeft = offsetWidth - menuMinWidth // left boundary
      const left = e.clientX - offsetLeft + 15 // 15: margin right

      if (left > maxLeft) {
        this.left = maxLeft
      } else {
        this.left = left
      }
      this.top = e.clientY

      // this.visible = true
      this.selectedTag = tag
    },

    handleCommand(command) {
      if (command === "0") {
        this.refreshSelectedTag(this.selectedTag)
      } else if (command === "1") {
        this.closeSelectedTag(this.selectedTag)
      } else if (command === "2") {
        this.closeOthersTags()
      } else {
        this.closeAllTags(this.selectedTag)
      }
    }

  }
}
</script>

<style lang="less" scoped>
.tags-view-container {
  height: 40px;
  width: 100%;
  background: #fff;
  border-bottom: 1px solid #d8dce5;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, .12), 0 0 3px 0 rgba(0, 0, 0, .04);

  .tags-view-wrapper {
    // line-height: 40px;
    .tags-view-item {
      display: inline-block;
      position: relative;
      cursor: pointer;
      height: 28px;
      line-height: 28px;
      border: 1px solid #d8dce5;
      color: #495060;
      background: #fff;
      padding: 0 8px;
      font-size: 12px;
      margin-left: 5px;
      margin-top: 4px;

      &:first-of-type {
        margin-left: 15px;
      }

      &:last-of-type {
        margin-right: 15px;
      }

      &.active {
        background-color: #42b983;
        color: #fff;
        border-color: #42b983;

        &::before {
          content: '';
          background: #fff;
          display: inline-block;
          width: 8px;
          height: 8px;
          border-radius: 50%;
          position: relative;
          margin-right: 2px;
        }
      }
    }
  }

  .contextmenu {
    margin: 0;
    background: #fff;
    z-index: 100;
    position: absolute;
    list-style-type: none;
    padding: 5px 0;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 400;
    color: #333;
    box-shadow: 2px 2px 3px 0 rgba(0, 0, 0, .3);

    li {
      margin: 0;
      padding: 7px 16px;
      cursor: pointer;

      &:hover {
        background: #eee;
      }
    }
  }
}
</style>

<style lang="less">
//reset element css of el-icon-close
.tags-view-wrapper {
  .tags-view-item {
    .el-icon-close {
      width: 16px;
      height: 16px;
      vertical-align: 2px;
      border-radius: 50%;
      text-align: center;
      transition: all .3s cubic-bezier(.645, .045, .355, 1);
      transform-origin: 100% 50%;

      &:before {
        transform: scale(.6);
        display: inline-block;
        vertical-align: -3px;
      }

      &:hover {
        background-color: #b4bccc;
        color: #fff;
      }
    }
  }
}

.el-dropdown-link {
  cursor: pointer;
  color: #409EFF;
}

.el-icon-arrow-down {
  font-size: 12px;
}
</style>
