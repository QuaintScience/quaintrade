import { AgGridReact } from 'ag-grid-react'; // React Grid Logic
import "ag-grid-community/styles/ag-grid.css"; // Core CSS
import "ag-grid-community/styles/ag-theme-quartz.css"; // Theme


export default function EditableJs({headers, data, handler, editable=true}) {
  var ag_grid_headers = headers.map((v) => ({"field": v, "editable": editable}));
  console.log(ag_grid_headers, data);
  return  (
    <div style={{height:'500px'}} >
        <AgGridReact className="ag-theme-quartz-dark" rowData={data} columnDefs={ag_grid_headers} />
    </div>
  );
};