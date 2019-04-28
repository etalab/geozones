<template>
<div class="card zone-details">
  <header class="card-header">
    <p class="card-header-title">{{ zone.properties.name }}</p>
    <span class="card-header-icon">
      <b-icon icon="draw-polygon"></b-icon>
    </span>
    <!--a href="#" class="card-header-icon" aria-label="more options">
    </a-->
  </header>
  <div class="card-content">
    <div class="content">
     <dl>
      <dt>ID</dt>
      <dd>{{ zone.properties.id }}</dd>

      <dt>Name</dt>
      <dd>{{ zone.properties.name }}</dd>

      <dt>Level</dt>
      <dd>{{ zone.properties.level }}</dd>

      <dt>Code</dt>
      <dd>{{ zone.properties.code }}</dd>

      <dt>Parents</dt>
      <dd v-for="parent in parents" :key="parent">{{ parent }}</dd>

      <dt>Known keys</dt>
      <dd>
        <dl>
          <template v-for="(value, key) in keys">
            <dt>{{ key }}</dt>
            <dd>{{ value }}</dd>
          </template>
        </dl>
      </dd>
      <!-- <dd v-for="(value, key) in keys" :key="key">{{ key }}</dd> -->

      <dt>Population</dt>
      <dd>{{ zone.properties.population }}</dd>

      <dt>Area (km2)</dt>
      <dd>{{ zone.properties.area }}</dd>
    </dl>
    </div>
  </div>
  <footer class="card-footer">
    <a href="#" class="card-footer-item">Save</a>
    <a href="#" class="card-footer-item">Edit</a>
    <a href="#" class="card-footer-item">Delete</a>
  </footer>
</div>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'

export default {
  computed: {
    keys() {
      return JSON.parse(this.zone.properties.keys)
    },
    parents() {
      return JSON.parse(this.zone.properties.parents)
    },
    ...mapGetters(['zone'])
  },
}
</script>

<style lang="postcss">
.zone-details {
  $indent: 100px;

  dt {
    float: left;
    text-align: right;
    font-weight: bold;

    &::after {
      content: ":";
    }
  }

  > dl {
    > dt {  /* First level only */
      width: $indent;
    }

    > dd {  /* First level only */
      margin: 0 0 0 calc($indent + 10px);
      padding: 0 0 0.2em 0;

      dt {
        width: 30px;
      }
    }
  }
}
</style>
