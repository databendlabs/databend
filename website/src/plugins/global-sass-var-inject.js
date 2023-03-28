const path = require('path');
module.exports = function (context, options) {
  return {
    name: 'custom-global-sass-var-inject-plugin',
    configureWebpack() {
      return {
        module: {
          rules: [
            {
              test: /\.scss$/,
              use: [
              {
                loader: 'sass-resources-loader',
                options: {
                  resources: [
                    path.resolve(__dirname,'../css/_mixins.scss')
                  ]
                }
              }]
            }
          ]
        }
      };
    }
  };
};