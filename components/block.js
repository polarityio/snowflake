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

  // ── JSON expansion (client-side, runs after Elixir processing) ────────────
  transformedPagedData: Ember.computed('pagedData.[]', function () {
    function looksLikeJson(val) {
      if (typeof val !== 'string' || val.length < 2) return false;
      const t = val.trim();
      return (t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'));
    }

    return (this.get('pagedData') || []).map(function (row) {
      const attrs = (row.attributes || []).map(function (attr) {
        if (!looksLikeJson(attr.value)) return attr;
        try {
          const parsed = JSON.parse(attr.value.trim());
          if (typeof parsed !== 'object' || parsed === null) return attr;
          let entries;
          if (Array.isArray(parsed)) {
            entries = parsed.map(function (item, i) {
              const v = typeof item === 'object' && item !== null ? JSON.stringify(item) : String(item == null ? 'null' : item);
              return { k: '[' + i + ']', v: v };
            });
          } else {
            entries = Object.keys(parsed).map(function (key) {
              const val = parsed[key];
              const v = typeof val === 'object' && val !== null ? JSON.stringify(val) : String(val == null ? 'null' : val);
              return { k: key, v: v };
            });
          }
          if (entries.length === 0) return attr;
          return { key: attr.key, value: attr.value, parsedEntries: entries };
        } catch (e) {
          return attr;
        }
      });
      return { index: row.index, title: row.title, attributes: attrs, raw: row.raw, resultAsString: row.resultAsString };
    });
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
    },
    copyText(text) {
      if (navigator && navigator.clipboard) {
        navigator.clipboard.writeText(text).catch(() => {});
      }
    }
  }
});
