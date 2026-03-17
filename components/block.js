'use strict';

polarity.export = PolarityComponent.extend({
  details: Ember.computed.alias('block.data.details'),
  errorMessage: '',

  // ── Paging ────────────────────────────────────────────────────────────────
  filterValue: '',
  currentPage: 1,
  pageSize: 5,

  pagingData: Ember.computed.alias('details.results'),

  filteredPagingData: Ember.computed('pagingData.[]', 'filterValue', function () {
    this.set('currentPage', 1);
    const filter = (this.get('filterValue') || '').toLowerCase().trim();
    if (!filter) return this.get('pagingData') || [];
    return (this.get('pagingData') || []).filter((row) =>
      (row.resultAsString || '').includes(filter)
    );
  }),

  isPrevDisabled: Ember.computed('currentPage', function () {
    return this.get('currentPage') === 1;
  }),

  isNextDisabled: Ember.computed('filteredPagingData.[]', 'pageSize', 'currentPage', function () {
    const total = this.get('filteredPagingData.length') || 0;
    const totalPages = Math.ceil(total / this.get('pageSize'));
    return this.get('currentPage') >= totalPages;
  }),

  pagingStartItem: Ember.computed('currentPage', 'pageSize', function () {
    return (this.get('currentPage') - 1) * this.get('pageSize') + 1;
  }),

  pagingEndItem: Ember.computed('pagingStartItem', 'pageSize', 'filteredPagingData.[]', function () {
    const end = this.get('pagingStartItem') - 1 + this.get('pageSize');
    return Math.min(end, this.get('filteredPagingData.length') || 0);
  }),

  pagedData: Ember.computed('filteredPagingData.[]', 'currentPage', 'pageSize', function () {
    const data = this.get('filteredPagingData') || [];
    const start = (this.get('currentPage') - 1) * this.get('pageSize');
    return data.slice(start, start + this.get('pageSize'));
  }),

  // ── State guards ──────────────────────────────────────────────────────────
  hasResults: Ember.computed('details.results.[]', function () {
    const r = this.get('details.results');
    return Array.isArray(r) && r.length > 0;
  }),

  hasFilteredResults: Ember.computed('filteredPagingData.[]', function () {
    return (this.get('filteredPagingData.length') || 0) > 0;
  }),

  showPaging: Ember.computed('filteredPagingData.[]', 'pageSize', function () {
    return (this.get('filteredPagingData.length') || 0) > this.get('pageSize');
  }),

  // ── Component lifecycle ───────────────────────────────────────────────────
  init() {
    this._super(...arguments);
    if (!this.get('block._state')) {
      this.set('block._state', {
        showMetadata: false,
        checkingStatus: false
      });
    }
  },

  // ── Actions ───────────────────────────────────────────────────────────────
  actions: {
    prevPage() {
      const page = this.get('currentPage');
      if (page > 1) this.set('currentPage', page - 1);
    },
    nextPage() {
      const total = this.get('filteredPagingData.length') || 0;
      const totalPages = Math.ceil(total / this.get('pageSize'));
      const page = this.get('currentPage');
      if (page < totalPages) this.set('currentPage', page + 1);
    },
    firstPage() {
      this.set('currentPage', 1);
    },
    lastPage() {
      const total = this.get('filteredPagingData.length') || 0;
      this.set('currentPage', Math.ceil(total / this.get('pageSize')));
    },
    toggleSection(key) {
      const path = `block._state.${key}`;
      this.set(path, !this.get(path));
    },
    checkQueryStatus() {
      this.set('block._state.checkingStatus', true);
      this.set('errorMessage', '');
      this.sendIntegrationMessage({ action: 'CHECK_QUERY_STATUS', statementHandle: this.get('details.statementHandle') })
        .then((result) => {
          this.set('block.data', result);
        })
        .catch((err) => {
          this.set('errorMessage', (err && err.detail) || 'Failed to check query status.');
        })
        .finally(() => {
          this.set('block._state.checkingStatus', false);
        });
    }
  }
});
