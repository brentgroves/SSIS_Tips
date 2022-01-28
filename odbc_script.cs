#region Help:  Introduction to the Script Component
/* The Script Component allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services data flow.
 *
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script component. */
#endregion

#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System.Data.Odbc;
#endregion

/// <summary>
/// This is the class to which to add your code.  Do not change the name, attributes, or parent
/// of this class.
/// </summary>
[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    #region Help:  Using Integration Services variables and parameters
    /* To use a variable in this script, first ensure that the variable has been added to
     * either the list contained in the ReadOnlyVariables property or the list contained in
     * the ReadWriteVariables property of this script component, according to whether or not your
     * code needs to write into the variable.  To do so, save this script, close this instance of
     * Visual Studio, and update the ReadOnlyVariables and ReadWriteVariables properties in the
     * Script Transformation Editor window.
     * To use a parameter in this script, follow the same steps. Parameters are always read-only.
     *
     * Example of reading from a variable or parameter:
     *  DateTime startTime = Variables.MyStartTime;
     *
     * Example of writing to a variable:
     *  Variables.myStringVariable = "new value";
     */
    #endregion

    #region Help:  Using Integration Services Connnection Managers
    /* Some types of connection managers can be used in this script component.  See the help topic
     * "Working with Connection Managers Programatically" for details.
     *
     * To use a connection manager in this script, first ensure that the connection manager has
     * been added to either the list of connection managers on the Connection Managers page of the
     * script component editor.  To add the connection manager, save this script, close this instance of
     * Visual Studio, and add the Connection Manager to the list.
     *
     * If the component needs to hold a connection open while processing rows, override the
     * AcquireConnections and ReleaseConnections methods.
     * 
     * Example of using an ADO.Net connection manager to acquire a SqlConnection:
     *  object rawConnection = Connections.SalesDB.AcquireConnection(transaction);
     *  SqlConnection salesDBConn = (SqlConnection)rawConnection;
     *
     * Example of using a File connection manager to acquire a file path:
     *  object rawConnection = Connections.Prices_zip.AcquireConnection(transaction);
     *  string filePath = (string)rawConnection;
     *
     * Example of releasing a connection manager:
     *  Connections.SalesDB.ReleaseConnection(rawConnection);
     */
    #endregion

    #region Help:  Firing Integration Services Events
    /* This script component can fire events.
     *
     * Example of firing an error event:
     *  ComponentMetaData.FireError(10, "Process Values", "Bad value", "", 0, out cancel);
     *
     * Example of firing an information event:
     *  ComponentMetaData.FireInformation(10, "Process Values", "Processing has started", "", 0, fireAgain);
     *
     * Example of firing a warning event:
     *  ComponentMetaData.FireWarning(10, "Process Values", "No rows were received", "", 0);
     */
    #endregion

    IDTSConnectionManager100 connMgr;
    OdbcConnection odbcConn;
    OdbcDataReader odbcDataReader;

    public override void AcquireConnections(object Transaction)
    {
        connMgr = this.Connections.Plex;
        odbcConn = (OdbcConnection)connMgr.AcquireConnection(null);

    }
    /// <summary>
    /// This method is called once, before rows begin to be processed in the data flow.
    ///
    /// You can remove this method if you don't need to do anything here.
    /// </summary>
    public override void PreExecute()
    {
        base.PreExecute();  // sproc300758_11728751_1958449= A ,call sproc300758_11728751_1942320 
        OdbcCommand cmd = new OdbcCommand("{call sproc300758_11728751_1898254(?,?,?)}", odbcConn);
        cmd.Parameters.Add("@PCN", OdbcType.Int);
        cmd.Parameters["@PCN"].Value = Variables.PCN;
        cmd.Parameters.Add("@By_Due_Date", OdbcType.NVarChar);
        //cmd.Parameters["@By_Due_Date"].Value = "20210701";
        string byDueDate = Variables.ByDueDate.ToString();
        cmd.Parameters["@By_Due_Date"].Value = byDueDate;
        cmd.Parameters.Add("@Building_Key", OdbcType.Int);
        cmd.Parameters["@Building_Key"].Value = Variables.BuildingKey;
        // Execute the DataReader and access the data.
        odbcDataReader = cmd.ExecuteReader();
    }

    /// <summary>
    /// This method is called after all the rows have passed through this component.
    ///
    /// You can delete this method if you don't need to do anything here.
    /// </summary>
    public override void PostExecute()
    {
        base.PostExecute();
        odbcDataReader.Close();
    }

    public override void CreateNewOutputRows()
    {
        while (odbcDataReader.Read())
        {
            OutputBuffer.AddRow();
            OutputBuffer.id = odbcDataReader.GetInt32(0);
            OutputBuffer.pcn = odbcDataReader.GetInt32(1);
            OutputBuffer.buildingkey = odbcDataReader.GetInt32(2);
            OutputBuffer.buildingcode = odbcDataReader.GetString(3);
            OutputBuffer.partkey = odbcDataReader.GetInt32(4);
            OutputBuffer.partno = odbcDataReader.GetString(5);
            OutputBuffer.name = odbcDataReader.GetString(6);
            OutputBuffer.qtyrel = odbcDataReader.GetInt32(7);
            OutputBuffer.qtyshipped = odbcDataReader.GetInt32(8);
            OutputBuffer.qtydue = odbcDataReader.GetInt32(9);
            OutputBuffer.pastdue = odbcDataReader.GetInt32(10);
            OutputBuffer.qtywip = odbcDataReader.GetInt32(11);
            OutputBuffer.qtyready = odbcDataReader.GetInt32(12);
            OutputBuffer.qtyloaded = odbcDataReader.GetInt32(13);
            OutputBuffer.qtyreadyorloaded = odbcDataReader.GetInt32(14);
        }
    }
    public override void ReleaseConnections()
    {

        connMgr.ReleaseConnection(odbcConn);

    }

}
