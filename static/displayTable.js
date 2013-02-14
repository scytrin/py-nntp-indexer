var DisplayTable = function(path, containerId, view) {
  this.query_path = path;
  this.container = document.getElementById(containerId);
  this.viewTweak = view;

  var url = document.location.origin + this.query_path;
  this.query = new google.visualization.Query(url);
  this.table = new google.visualization.Table(this.container);
  this.dataTable = null;
  this.viewTable = null;
  this.pageSize = 100;
  this.currentPage = 0;

  var self = this;
  var addListener = google.visualization.events.addListener;
  addListener(this.table, 'page', function(e) {self.handlePage(e)});
};

DisplayTable.prototype.handleResponse = function(res) {
  this.dataTable = null;
  this.viewTable = null;
  if (res.isError()) {
    google.visualization.errors.addError(this.container, res.getMessage(),
        res.getDetailedMessage(), {'showInTooltip': false});
  } else {
    this.dataTable = res.getDataTable();
    this.viewTable = new google.visualization.DataView(this.dataTable);
    if (this.viewTweak) this.viewTweak(this.viewTable);
    this.table.draw(this.viewTable, {
      page: 'event',
      sort: 'disable',
      pagingButtonsConfiguration: 'both',
      pageSize: this.pageSize,
      showRowNumber: false
    });
  }
};

DisplayTable.prototype.sendAndDraw = function() {
  this.table.setSelection([]);
  this.query.abort();

  var pageClause = 'limit ' + (this.pageSize + 1) + ' offset ' + (this.pageSize * this.currentPage);
  this.query.setQuery(pageClause);

  var self = this;
  this.query.send(function(res) { self.handleResponse(res); });
};

DisplayTable.prototype.handlePage = function(e) {
  var newPage = e.page ? this.currentPage + e.page : 0; // e.page >> -1, 0, 1
  console.log([e.page, this.currentPage, newPage]);
  if (newPage < 0)
    return false;
  if (newPage > this.currentPage && this.viewTable) {
    console.log([this.viewTable.getNumberOfRows(), this.pageSize]);
    if (this.viewTable.getNumberOfRows() <= this.pageSize)
      return false;
  }
  this.currentPage = newPage;
  this.sendAndDraw();
};

DisplayTable.prototype.formSendAndDraw = function(form_element) {
  var self = this;
  return function() {
    var query_str = $(this).serialize();
    var url = document.location.origin + self.query_path + '?' + query_str;
    self.query = new google.visualization.Query(url);
    self.handlePage({'page': 0});
    return false;
  };
};
