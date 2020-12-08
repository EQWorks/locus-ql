const {
  getCampView,
  getOsView,
  getBrowserView,
  getBannerView,
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

// sorted from lower to higher cardinality
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

  ATOM_CAMPS: 12,
  ATOM_OS: 13,
  ATOM_BANNERS: 14,
  ATOM_BROWSERS: 15,

  LOCUS_CAMPS: 16,
  LOCUS_BEACONS: 17,
  LOCUS_BEACON_HISTORY: 18,
  LOCUS_POI: 19,
  LOCUS_POI_LISTS: 20,
  LOCUS_SEGMENTS: 21,
}

const pgViewGetters = {
  // standard ch views
  [pgViews.ATOM_CH_AD_POSITION]: cuID => makeChView('adposition', 'ad_position', 'int', cuID),
  [pgViews.ATOM_CH_BROWSER_ID]: cuID => makeChView('browserid', 'browser_id', 'int', cuID),
  [pgViews.ATOM_CH_OS_ID]: cuID => makeChView('osid', 'os_id', 'int', cuID),
  [pgViews.ATOM_CH_LANGUAGE]: cuID => makeChView('language', 'language', 'text', cuID),
  [pgViews.ATOM_CH_CITY]: cuID => makeChView('city', 'city', 'text', cuID),
  [pgViews.ATOM_CH_BANNER_CODE]: cuID => makeChView('bannercode', 'banner_code', 'int', cuID),
  [pgViews.ATOM_CH_DOMAIN_ID]: cuID => makeChView('domainid', 'domain_id', 'int', cuID),
  [pgViews.ATOM_CH_IAB_CAT]: cuID => makeChView('iabcat', 'iab_cat', 'text', cuID),
  [pgViews.ATOM_CH_NETWORK_ID]: cuID => makeChView('ntwrkid', 'network_id', 'int', cuID),
  [pgViews.ATOM_CH_SESS_DEPTH]: cuID => makeChView('session_depth', 'session_depth', 'int', cuID),
  [pgViews.ATOM_CH_USER_SEG]: cuID => makeChView('user_segment', 'user_segment_id', 'int', cuID),
  // insert here non-standard views (viewability + vast)

  [pgViews.ATOM_CAMPS]: getCampView,
  [pgViews.ATOM_OS]: getOsView,
  [pgViews.ATOM_BANNERS]: getBannerView,
  [pgViews.ATOM_BROWSERS]: getBrowserView,

  [pgViews.LOCUS_CAMPS]: getLocusCampView,
  [pgViews.LOCUS_BEACONS]: getBeaconView,
  [pgViews.LOCUS_BEACON_HISTORY]: getBeaconHistoryView,
  [pgViews.LOCUS_POI]: getPOIView,
  [pgViews.LOCUS_POI_LISTS]: getPOIListView,
  [pgViews.LOCUS_SEGMENTS]: getSegmentView,
}

const getPgView = (view, customerID) => pgViewGetters[view](customerID)

module.exports = {
  pgViews,
  getPgView,
}
