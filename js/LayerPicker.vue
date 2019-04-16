<template>
<div class="layer-picker">
    <a href v-if="!expanded" @click.prevent="expand">
        ☰ {{ level ? level.label : 'No level selected' }}
    </a>
    <a class="choice" href v-if="expanded" v-for="level in levels" @click.prevent="pick(level)" :key="level.id">
        {{ level.label }}
    </a>
</div>    
</template>

<script>
import {mapGetters, mapActions} from 'vuex'
export default {
    data() {
        return {expanded: false}
    },
    computed: {
        ...mapGetters(['level', 'levels'])
    },
    methods: {
        expand() {
            this.expanded = true
        },
        pick(level) {
            this.setLevel(level)
            this.expanded = false
        },
        ...mapActions(['setLevel'])
    }
}
</script>

<style lang="postcss">
.layer-picker {
    background-color: white;

    display: flex;
    flex-direction: column;

    a {
        display: block;
        text-decoration: none;

        &.choice {
            background-color: teal;
            color: white;
            margin: 2px;
            padding: 2px;
            border-radius: 2px;
        }
    }
}
</style>
