<template>
<div class="card zone-details">
  <header class="card-header">
    <p class="card-header-title is-centered" title="Name">{{ zone.properties.name }}</p>
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

        <dt>Level</dt>
        <dd>{{ zone.properties.level }}</dd>

        <dt>Code</dt>
        <dd>{{ zone.properties.code }}</dd>

        <dt v-if="zone.properties.capital">Capital</dt>
        <dd v-if="zone.properties.capital">
          <a href @click.prevent="setZone(zone.properties.capital)">{{ zone.properties.capital }}</a>
        </dd>

        <dt v-if="parents">Parents</dt>
        <dd v-for="parent in parents" :key="parent">
          <a href @click.prevent="setZone(parent)">{{ parent }}</a>
        </dd>

        <dt v-if="ancestors">Ancestor(s)</dt>
        <dd v-for="ancestor in ancestors" :key="ancestor">
          <a href @click.prevent="setZone(ancestor)">{{ ancestor }}</a>
        </dd>

        <dt v-if="successors">successor(s)</dt>
        <dd v-for="successor in successors" :key="successor">
          <a href @click.prevent="setZone(successor)">{{ successor }}</a>
        </dd>

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

        <dt>Validity</dt>
        <dd v-if="!validity || !(validity.start || validity.end)">N/A</dd>
        <dd v-else-if="validity.start && !validity.end">Since {{ validity.start }}</dd>
        <dd v-else-if="!validity.start && validity.end">Until {{ validity.end }}</dd>
        <dd v-else>{{ validity.start }} - {{ validity.end }}</dd>
      </dl>
    </div>
  </div>
  <footer class="card-footer">
    <a v-if="wikipedia" :href="wikipedia" class="card-footer-item">Wikipedia</a>
    <a v-if="wikidata" :href="wikidata" class="card-footer-item">Wikidata</a>
    <a v-if="zone.properties.website" :href="zone.properties.website" class="card-footer-item">Website</a>
  </footer>
</div>
</template>

<script>
import {mapActions, mapState} from 'vuex'

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
    ancestors() {
      if (typeof this.zone.properties.ancestors === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.ancestors)
      }
      return this.zone.properties.ancestors
    },
    successors() {
      if (typeof this.zone.properties.successors === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.successors)
      }
      return this.zone.properties.successors
    },
    parents() {
      if (typeof this.zone.properties.parents === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.parents)
      }
      return this.zone.properties.parents
    },
    validity() {
      if (typeof this.zone.properties.validity === 'string') {  // MapboxGL fix
        return JSON.parse(this.zone.properties.validity)
      }
      return this.zone.properties.validity
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
    wikidata() {
      if (!this.zone.properties.wikidata) return
      return `https://www.wikidata.org/wiki/${this.zone.properties.wikidata}`
    },
    ...mapState(['zone'])
  },
  methods: {
    dbpediaMedia: filename => filename.startsWith('http') ? filename : `${DBPEDIA_MEDIA_URL}${filename}`,
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
      $indent: 70px;
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
