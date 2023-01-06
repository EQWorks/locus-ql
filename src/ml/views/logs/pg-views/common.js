const { knex } = require('../../../../util/db')
const iabCats = require('./iab.json')


const getIABCatView = () => {
  const apostrophe = /'|’/g
  const iabValues = iabCats.reduce((values, { code, name, children }) => {
    const safeName = name.replace(apostrophe, '\'\'')
    values.push(`('${code}', '${safeName}', '${code}', '${safeName}')`)
    if (!children) {
      return values
    }
    return values.concat(children
      // eslint-disable-next-line max-len
      .map(child => `('${child.code}', '${child.name.replace(apostrophe, '\'\'')}', '${code}', '${safeName}')`))
  }, [])

  return {
    view: knex.raw(`
      (
        SELECT * FROM (
          VALUES
            ${iabValues.join(', ')}
        ) AS t (iab_cat, iab_cat_name, iab_parent_cat, iab_parent_cat_name)
      ) AS iab_cats
    `),
  }
}

const getAppPlatformView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM (
        VALUES
          (0, 'Unknown/Browser'),
          (1, 'Android'),
          (2, 'iOS')
      ) AS t (app_platform_id, app_platform_name)
    ) AS app_platforms
  `),
})

const getMaxMindConnectionTypeView = () => ({
  view: knex.raw(`
    (
      SELECT * FROM (
        VALUES
          (3, 'Corporate'),
          (4, 'Cable/DSL/Dialup'),
          (5, 'Cellular')
      ) AS t (connection_type, connection_type_name)
    ) AS maxmind_connection_types
  `),
})

module.exports = {
  getIABCatView,
  getAppPlatformView,
  getMaxMindConnectionTypeView,
}
