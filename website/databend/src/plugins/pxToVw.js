module.exports = function (context, options) {
  return {
    configurePostCss(postcssOptions) {
      postcssOptions.plugins.push(
        require('postcss-px-to-viewport')({
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
      return postcssOptions
    },
  }
}