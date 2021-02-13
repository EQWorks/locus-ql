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
} = require('./locus')
const {
  getIABCatView,
  getAppPlatformView,
  getMaxMindConnectionTypeView,
} = require('./common')


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

  APP_PLATFORMS: 28,
  MAXMIND_CONNECTION_TYPES: 29,
  IAB_CATS: 30,
}

const pgViewGetters = {
  // standard ch views
  [pgViews.ATOM_CH_AD_POSITION]: (_, ad) => makeChView('adposition', 'ad_position', 'int', ad),
  [pgViews.ATOM_CH_BROWSER_ID]: (_, ad) => makeChView('browserid', 'browser_id', 'int', ad),
  [pgViews.ATOM_CH_OS_ID]: (_, ad) => makeChView('osid', 'os_id', 'int', ad),
  [pgViews.ATOM_CH_LANGUAGE]: (_, ad) => makeChView('language', 'language_code', 'text', ad),
  [pgViews.ATOM_CH_CITY]: (_, ad) => makeChView('city', 'city', 'text', ad),
  [pgViews.ATOM_CH_BANNER_CODE]: (_, ad) => makeChView('bannercode', 'banner_code', 'int', ad),
  [pgViews.ATOM_CH_DOMAIN_ID]: (_, ad) => makeChView('domainid', 'domain_id', 'int', ad),
  [pgViews.ATOM_CH_IAB_CAT]: (_, ad) => makeChView('iabcat', 'iab_cat', 'text', ad),
  [pgViews.ATOM_CH_NETWORK_ID]: (_, ad) => makeChView('ntwrkid', 'network_id', 'int', ad),
  [pgViews.ATOM_CH_SESS_DEPTH]: (_, ad) => makeChView('session_depth', 'session_depth', 'int', ad),
  [pgViews.ATOM_CH_USER_SEG]: (_, ad) => makeChView('user_segment', 'user_segment_id', 'int', ad),
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

  [pgViews.APP_PLATFORMS]: getAppPlatformView,
  [pgViews.MAXMIND_CONNECTION_TYPES]: getMaxMindConnectionTypeView,
  [pgViews.IAB_CATS]: getIABCatView,
}

const getPgView = (view, agencyID, advertiserID) => pgViewGetters[view](agencyID, advertiserID)

module.exports = {
  pgViews,
  getPgView,
}
