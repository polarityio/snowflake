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

  // ── Tab class helpers ─────────────────────────────────────────────────────
  cardsTabClass: Ember.computed('block._state.activeTab', function () {
    return this.get('block._state.activeTab') === 'cards' ? 'snow-tab snow-tab-active' : 'snow-tab';
  }),

  tableTabClass: Ember.computed('block._state.activeTab', function () {
    return this.get('block._state.activeTab') === 'table' ? 'snow-tab snow-tab-active' : 'snow-tab';
  }),

  // ── Copy-button visual feedback ──────────────────────────────────────────
  copyHandleIcon: Ember.computed('block._state.copiedHandle', function () {
    return this.get('block._state.copiedHandle') ? 'check' : 'copy';
  }),

  copyQueryIcon: Ember.computed('block._state.copiedQuery', function () {
    return this.get('block._state.copiedQuery') ? 'check' : 'copy';
  }),

  // ── Component lifecycle ───────────────────────────────────────────────────
  init() {
    this._super(...arguments);
    if (!this.get('block._state')) {
      this.set('block._state', {
        showMetadata: false,
        showRenderedQuery: false,
        copiedHandle: false,
        copiedQuery: false,
        activeTab: 'cards'
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
    switchTab(tab) {
      this.set('block._state.activeTab', tab);
    },

    /**
     * Copies arbitrary text to the clipboard and sets a transient flag so
     * the icon can switch to a checkmark for ~1.5s. Pure browser-side; no
     * onMessage hop, no new dependency. See production usage in
     * polarityio/dataminr-pulse, polarityio/cyberchef, polarityio/netscout-vast.
     */
    copyToClipboard(text, flag) {
      const self = this;
      const flagPath = `block._state.${flag}`;

      const onCopied = () => {
        self.set(flagPath, true);
        Ember.run.later(() => {
          if (!self.isDestroyed && !self.isDestroying) self.set(flagPath, false);
        }, 1500);
      };

      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(onCopied).catch(() => {
            // Fallback to execCommand
            self._fallbackCopy(text, onCopied);
          });
        } else {
          self._fallbackCopy(text, onCopied);
        }
      } catch (e) {
        self.set('errorMessage', 'Clipboard copy failed in this browser.');
      }
    }
  },

  _fallbackCopy(text, onCopied) {
    try {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'absolute';
      textarea.style.left = '-9999px';
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      onCopied();
    } catch (e) {
      this.set('errorMessage', 'Clipboard copy failed in this browser.');
    }
  }
});
