$(document).ready(function () {
  const filialSelect = $('#filialSelect');
  const exportBtn = $('#exportBtn');
  let dataTable = null;

  function fetchAndRender(filial) {
    // Clear previous table if exists
    if (dataTable) {
      dataTable.destroy();
      $('#tenderTable tbody').empty();
      $('#tenderTable thead tr').empty();
    }
    exportBtn.prop('disabled', true);
    $.getJSON(`/data?filial=${encodeURIComponent(filial)}`, function (data) {
      const rows = data[filial] || [];
      if (rows.length === 0) {
        // No data for selected filial
        return;
      }
      // Determine table columns from keys of first row
      const columns = Object.keys(rows[0]);
      // Insert header cells (first column for checkbox)
      $('#tenderTable thead tr').append('<th></th>');
      columns.forEach((col) => {
        $('#tenderTable thead tr').append(`<th>${col}</th>`);
      });
      // Build data array for DataTables
      const dataSet = rows.map((row, idx) => {
        const checkbox = `<input type="checkbox" class="row-select" data-index="${idx}" />`;
        return [checkbox, ...columns.map((col) => row[col] === undefined ? '' : row[col])];
      });
      // Initialize DataTable
      dataTable = $('#tenderTable').DataTable({
        data: dataSet,
        columns: [{ title: '' }, ...columns.map((col) => ({ title: col }))],
        columnDefs: [{ orderable: false, targets: 0 }],
        order: [[1, 'asc']],
        language: {
          url: 'https://cdn.datatables.net/plug-ins/1.13.6/i18n/ru.json',
        },
      });
      exportBtn.prop('disabled', false);
    });
  }

  // On filial change
  filialSelect.on('change', function () {
    const selected = $(this).val();
    fetchAndRender(selected);
  });

  // Export button
  exportBtn.on('click', function () {
    const filial = filialSelect.val();
    // Collect selected indices
    const checked = [];
    $('#tenderTable tbody input.row-select:checked').each(function () {
      checked.push($(this).data('index'));
    });
    let url = `/export?filial=${encodeURIComponent(filial)}`;
    if (checked.length > 0) {
      url += `&indices=${checked.join(',')}`;
    }
    // Initiate file download
    window.location.href = url;
  });

  // Initial render for the first filial
  const initialFilial = filialSelect.val();
  if (initialFilial) {
    fetchAndRender(initialFilial);
  }
});