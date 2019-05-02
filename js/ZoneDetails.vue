<template>
<div class="card zone-details">
  <header class="card-header">
    <p class="card-header-title is-centered">{{ zone.properties.name }}</p>
  </header>
  <div class="card-content">
    <div v-if="hasLogos" class="columns logos">
      <div class="column has-text-centered" v-if="zone.properties.flag">
        <img :src="dbpediaMedia(zone.properties.flag)" alt="Flag" />
      </div>
      <div class="column has-text-centered" v-if="zone.properties.blazon">
        <img :src="dbpediaMedia(zone.properties.blazon)" alt="Blazon" />
      </div>
      <div class="column has-text-centered" v-if="zone.properties.logo">
        <img :src="dbpediaMedia(zone.properties.logo)" alt="Logo"/>
      </div>
    </div>
    <div class="content">
      <dl class="props">
        <dt>ID</dt>
        <dd>{{ zone.properties.id }}</dd>

        <dt>Name</dt>
        <dd>{{ zone.properties.name }}</dd>

        <dt>Level</dt>
        <dd>{{ zone.properties.level }}</dd>

        <dt>Code</dt>
        <dd>{{ zone.properties.code }}</dd>

        <dt>Parents</dt>
        <dd v-for="parent in parents" :key="parent"><a href @click.prevent="setZone(parent)">{{ parent }}</a></dd>

        <dt>Keys</dt>
        <dd>
          <dl class="keys">
            <template v-for="(value, key) in keys">
              <dt>{{ key }}</dt>
              <dd>{{ formatValue(value) }}</dd>
            </template>
          </dl>
        </dd>

        <dt>Population</dt>
        <dd>
          <span v-if="zone.properties.population">{{ zone.properties.population }}</span>
          <span v-else class="has-text-danger"><b-icon icon="times"></b-icon></span>
        </dd>

        <dt>Area (km2)</dt>
        <dd>
          <span v-if="zone.properties.area">{{ zone.properties.area }}</span>
          <span v-else class="has-text-danger"><b-icon icon="times"></b-icon></span>
        </dd>
      </dl>
    </div>
  </div>
  <footer class="card-footer">
    <a v-if="wikipedia" :href="wikipedia" class="card-footer-item">Wikipedia</a>
    <!--a href="#" class="card-footer-item">Edit</a>
    <a href="#" class="card-footer-item">Delete</a-->
  </footer>
</div>
</template>

<script>
import {mapGetters, mapActions} from 'vuex'

const DBPEDIA_MEDIA_URL = 'https://commons.wikimedia.org/wiki/Special:FilePath/'

export default {
  computed: {
    hasLogos() {
      return this.zone.properties.logo || this.zone.properties.flag || this.zone.properties.blazon
    },
    keys() {
      if (typeof this.zone.properties.keys === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.keys)
      }
      return this.zone.properties.keys
    },
    parents() {
      if (typeof this.zone.properties.parents === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.parents)
      }
      return this.zone.properties.parents
    },
    wikipedia() {
      if (!this.zone.properties.wikipedia) return
      if (this.zone.properties.wikipedia.includes(':')) {
        const [ns, path] = this.zone.properties.wikipedia.split(':')
        return `https://${ns}.wikipedia.org/wiki/${path}`
      } else {
        return `https://wikipedia.org/wiki/${path}`
      }
    },
    ...mapGetters(['zone'])
  },
  methods: {
    dbpediaMedia: filename => `${DBPEDIA_MEDIA_URL}${filename}`,
    formatValue: value => Array.isArray(value) ? value.join(', ') : value,
    ...mapActions(['setZone'])
  }
}
</script>

<style lang="postcss">
.zone-details {
  .logos {
    max-height: 100px;
    img {
      max-height: 100%;
    }
  }

  .content {
    font-size: 14px;
    $indent: 80px;

    dt {
      float: left;
      text-align: right;
      font-weight: bold;

      &::after {
        content: ": ";
      }
    }

    dl.props {
      > dt {  /* First level only */
        width: $indent;
      }

      > dd {  /* First level only */
        margin: 0 0 0 calc($indent + 10px);
        padding: 0 0 0.2em 0;


      }
    }

    dl.keys {
      $indent: 45px;
      dt {
        width: $indent;
      }
      dd {
        margin: 0 0 0 calc($indent + 5px);
      }
    }
  }

}
</style>
