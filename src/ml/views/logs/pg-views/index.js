const {
  getCampView,
  getOsView,
  getBrowserView,
  getBannerView,
  getAdPositionView,
  getDomainView,
  getSegmentView: getAtomSegmentView,
  getLanguageView,
  getNetworkView,
  getChViewabilityView,
  makeChView,
} = require('./atom')
const {
  getCampView: getLocusCampView,
  getBeaconView,
  getBeaconHistoryView,
  getPOIView,
  getPOIListView,
  getSegmentView,
  getGeoCohortView,
  getGeoCohortItemView,
} = require('./locus')
const {
  getIABCatView,
  getAppPlatformView,
  getMaxMindConnectionTypeView,
} = require('./common')


// max enum value: 33
// incr. when adding a new item
/**
 * @enum
 */
const pgViews = {
  ATOM_CH_AD_POSITION: 1,
  ATOM_CH_BROWSER_ID: 2,
  ATOM_CH_OS_ID: 3,
  ATOM_CH_LANGUAGE: 4,
  ATOM_CH_CITY: 5,
  ATOM_CH_BANNER_CODE: 6,
  ATOM_CH_DOMAIN_ID: 7,
  ATOM_CH_IAB_CAT: 8,
  ATOM_CH_NETWORK_ID: 9,
  ATOM_CH_SESS_DEPTH: 10,
  ATOM_CH_USER_SEG: 11,
  ATOM_CH_GEOCOHORT_ID_ITEM: 31,
  ATOM_CH_VIEWABILITY: 27,

  ATOM_CAMPS: 12,
  ATOM_OS: 13,
  ATOM_BANNERS: 14,
  ATOM_BROWSERS: 15,
  ATOM_AD_POSITIONS: 22,
  ATOM_DOMAINS: 23,
  ATOM_SEGMENTS: 24,
  ATOM_LANGUAGES: 25,
  ATOM_NETWORKS: 26,

  LOCUS_CAMPS: 16,
  LOCUS_BEACONS: 17,
  LOCUS_BEACON_HISTORY: 18,
  LOCUS_POI: 19,
  LOCUS_POI_LISTS: 20,
  LOCUS_SEGMENTS: 21,
  LOCUS_GEO_COHORTS: 32,
  LOCUS_GEO_COHORT_ITEMS: 33,

  APP_PLATFORMS: 28,
  MAXMIND_CONNECTION_TYPES: 29,
  IAB_CATS: 30,
}

const pgViewGetters = {
  // standard ch views
  [pgViews.ATOM_CH_AD_POSITION]: makeChView([['adposition', 'ad_position', 'int']]),
  [pgViews.ATOM_CH_BROWSER_ID]: makeChView([['browserid', 'browser_id', 'int']]),
  [pgViews.ATOM_CH_OS_ID]: makeChView([['osid', 'os_id', 'int']]),
  [pgViews.ATOM_CH_LANGUAGE]: makeChView([['language', 'language_code', 'text']]),
  [pgViews.ATOM_CH_CITY]: makeChView([['city', 'city', 'text']]),
  [pgViews.ATOM_CH_BANNER_CODE]: makeChView([['bannercode', 'banner_code', 'int']]),
  [pgViews.ATOM_CH_DOMAIN_ID]: makeChView([['domainid', 'domain_id', 'int']]),
  [pgViews.ATOM_CH_IAB_CAT]: makeChView([['iabcat', 'iab_cat', 'text']]),
  [pgViews.ATOM_CH_NETWORK_ID]: makeChView([['ntwrkid', 'network_id', 'int']]),
  [pgViews.ATOM_CH_SESS_DEPTH]: makeChView([['session_depth', 'session_depth', 'int']]),
  [pgViews.ATOM_CH_USER_SEG]: makeChView([['user_segment', 'user_segment_id', 'int']]),
  [pgViews.ATOM_CH_GEOCOHORT_ID_ITEM]: makeChView([
    ['geocohortlistid', 'geo_cohort_id', 'int'],
    ['geocohortitem', '_geo_cohort_item', 'text'],
  ]),
  // insert here non-standard views (viewability + vast)
  [pgViews.ATOM_CH_VIEWABILITY]: getChViewabilityView,

  [pgViews.ATOM_CAMPS]: getCampView,
  [pgViews.ATOM_OS]: getOsView,
  [pgViews.ATOM_BANNERS]: getBannerView,
  [pgViews.ATOM_BROWSERS]: getBrowserView,
  [pgViews.ATOM_AD_POSITIONS]: getAdPositionView,
  [pgViews.ATOM_DOMAINS]: getDomainView,
  [pgViews.ATOM_SEGMENTS]: getAtomSegmentView,
  [pgViews.ATOM_LANGUAGES]: getLanguageView,
  [pgViews.ATOM_NETWORKS]: getNetworkView,

  [pgViews.LOCUS_CAMPS]: getLocusCampView,
  [pgViews.LOCUS_BEACONS]: getBeaconView,
  [pgViews.LOCUS_BEACON_HISTORY]: getBeaconHistoryView,
  [pgViews.LOCUS_POI]: getPOIView,
  [pgViews.LOCUS_POI_LISTS]: getPOIListView,
  [pgViews.LOCUS_SEGMENTS]: getSegmentView,
  [pgViews.LOCUS_GEO_COHORTS]: getGeoCohortView,
  [pgViews.LOCUS_GEO_COHORT_ITEMS]: getGeoCohortItemView,

  [pgViews.APP_PLATFORMS]: getAppPlatformView,
  [pgViews.MAXMIND_CONNECTION_TYPES]: getMaxMindConnectionTypeView,
  [pgViews.IAB_CATS]: getIABCatView,
}

const getPgView = (view, agencyID, advertiserID, engine = 'pg') =>
  pgViewGetters[view]({ agencyID, advertiserID, engine })

module.exports = {
  pgViews,
  getPgView,
}
