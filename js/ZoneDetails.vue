<template>
<div class="zone-details">
  <h1 id="title">{{ zone.properties.name }}</h1>
  <dl>
    <dt>ID</dt>
    <dd>{{ id }}</dd>

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
</template>

<script>
import {mapGetters, mapActions} from 'vuex'

export default {
  computed: {
    id() {
      return `${this.zone.properties.level}:${this.zone.properties.code}`
    },
    keys() {
      return JSON.parse(this.zone.properties.keys)
      // const keys = JSON.parse(this.zone.properties.keys)
      // return Object.keys(keys).map(key => key + ': ' + keys[key])
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
  h1 {
    text-align: center;
    margin-top: 0;
  }

  $indent: 100px;

  dt {
    float: left;
    /* clear: left; */
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
