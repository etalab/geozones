/* global module,process */
const path = require('path')
const webpack = require('webpack')
const VueLoaderPlugin = require('vue-loader/lib/plugin')

const PATH_PREFIX = process.env.PATH_PREFIX || '';

const css_loaders = [
  {loader: 'vue-style-loader', options: {sourceMap: true}},
  {loader: 'css-loader', options: {sourceMap: true, importLoaders: true}},
  {loader: 'postcss-loader', options: {sourceMap: true}},
]

module.exports = function(env, argv) {
  const isProd = argv.mode === 'production'
  const config = {
    entry: ['@babel/polyfill', './js/index.js'],
    output: {
      path: path.resolve(__dirname, './geozones/static/'),
      publicPath: `${PATH_PREFIX}/static/`,
      filename: 'ui.js'
    },
    resolve: {
      extensions: ['.js', '.vue'],
      alias: {
        'vue$': 'vue/dist/vue.esm.js',
        'public': path.resolve(__dirname, './public')
      }
    },
    module: {
      rules: [
        {test: /\.vue$/, loader: 'vue-loader'},
        {test: /\.js$/, exclude: /node_modules/, loader: 'babel-loader'},
        {test: /\.(post)?css$/, use: css_loaders},
        {
          test: /\.(png|jpg|gif|svg)$/,
          loader: 'file-loader',
          options: {
            objectAssign: 'Object.assign'
          }
        },
      ]
    },
    devServer: {
      historyApiFallback: true,
      contentBase: path.resolve(__dirname),
      noInfo: true,
      overlay: {
          warnings: true,
          errors: true
      },
      hot: true
    },
    performance: {
      hints: false
    },
    devtool: isProd ? 'source-map' : 'eval-source-map',
    plugins: [
      new webpack.DefinePlugin({
        PRODUCTION: JSON.stringify(isProd)
      }),
      new VueLoaderPlugin(),
      new webpack.ProvidePlugin({
        mapboxgl: 'mapbox-gl'
      })
    ]
  }

  return config
}
