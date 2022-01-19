module.exports = function (context, options) {
  return {
    configurePostCss(postcssOptions) {
      try {
        postcssOptions.plugins.push(
          require('postcss-px-to-viewport-8-plugin')({
            exclude: [/node_modules/],
            minPixelValue: 1,
            unitPrecision: 7, 
            mediaQuery: true,
            viewportWidth: 1920,
            selectorBlackList: [
              '::-webkit-scrollbar',
              '[ignore]'
            ],
          }),
        )
        return postcssOptions;
      } catch (error) {
        console.log(`${error}, perhaps you have not installed the postcss-px-to-viewport-8-plugin, please run "yarn add postcss-px-to-viewport-8-plugin"`);
      }
    },
  }
}