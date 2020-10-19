using System;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace Atomus.Database
{
    public class Oracle : IDatabase
    {
        OracleDataAdapter sqlDataAdapter;

        public Oracle()
        {
            this.sqlDataAdapter = new OracleDataAdapter
            {
                SelectCommand = new OracleCommand
                {
                    Connection = new OracleConnection()
                }
            };
            //this.sqlDataAdapter.SelectCommand.Connection = new SqlConnection();
        }

        DbParameter IDatabase.AddParameter(string parameterName, DbType dbType, int size)
        {
            OracleCommand sqlCommand;

            try
            {
                sqlCommand = this.sqlDataAdapter.SelectCommand;

                if (size == 0)
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType));
                else
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType), size);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        OracleDbType DbTypeConvert(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.BigInt:
                    throw new AtomusException("DbType.BigInt type Not Support.");

                case DbType.Binary:
                    return OracleDbType.LongRaw;

                case DbType.Bit:
                    throw new AtomusException("DbType.Bit type Not Support.");

                case DbType.Char:
                    return OracleDbType.Char;

                case DbType.Date:
                    return OracleDbType.Date;
                    //throw new AtomusException("DbType.Date type Not Support.");

                case DbType.DateTime:
                    return OracleDbType.Date;

                case DbType.DateTime2:
                    return OracleDbType.Date;
                    //throw new AtomusException("DbType.DateTime2 type Not Support.");

                case DbType.DateTimeOffset:
                    throw new AtomusException("DbType.DateTimeOffset type Not Support.");

                case DbType.Decimal:
                    return OracleDbType.Decimal;

                case DbType.Float:
                    return OracleDbType.Double;

                case DbType.Image:
                    throw new AtomusException("DbType.Image type Not Support.");

                case DbType.Int:
                    return OracleDbType.Int32;

                case DbType.Money:
                    throw new AtomusException("DbType.Money type Not Support.");

                case DbType.NChar:
                    return OracleDbType.NChar;

                case DbType.NText:
                    return OracleDbType.NClob;
                    //throw new AtomusException("DbType.NText type Not Support.");

                case DbType.NVarChar:
                    return OracleDbType.NChar;

                case DbType.Real:
                    throw new AtomusException("DbType.Real type Not Support.");

                case DbType.SmallDateTime:
                    throw new AtomusException("DbType.SmallDateTime type Not Support.");

                case DbType.SmallInt:
                    return OracleDbType.Int16;

                case DbType.SmallMoney:
                    throw new AtomusException("DbType.SmallMoney type Not Support.");

                case DbType.Structured:
                    throw new AtomusException("DbType.Structured type Not Support.");

                case DbType.Text:
                    return OracleDbType.Clob;
                    //throw new AtomusException("DbType.Text type Not Support.");

                case DbType.Time:
                    throw new AtomusException("DbType.Time type Not Support.");

                case DbType.Timestamp:
                    return OracleDbType.TimeStamp;

                case DbType.TinyInt:
                    return OracleDbType.Byte;

                case DbType.Udt:
                    throw new AtomusException("DbType.Udt type Not Support.");

                case DbType.UniqueIdentifier:
                    throw new AtomusException("DbType.UniqueIdentifier type Not Support.");

                case DbType.VarBinary:
                    throw new AtomusException("DbType.VarBinary type Not Support.");

                case DbType.VarChar:
                    return OracleDbType.Varchar2;

                case DbType.Variant:
                    throw new AtomusException("DbType.Variant type Not Support.");

                case DbType.Xml:
                    throw new AtomusException("DbType.Xml type Not Support.");

                default:
                    throw new AtomusException("type Not Support.");
            }
        }

        DbCommand IDatabase.Command
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand;
            }
        }

        DbConnection IDatabase.Connection
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Connection;
            }
        }

        DbDataAdapter IDatabase.DataAdapter
        {
            get
            {
                return this.sqlDataAdapter;
            }
        }

        DbTransaction IDatabase.Transaction
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Transaction;
            }
        }

        void IDatabase.DeriveParameters()
        {
            try
            {
                OracleCommandBuilder.DeriveParameters(this.sqlDataAdapter.SelectCommand);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        void IDatabase.Close()
        {
            try
            {
                if (this.sqlDataAdapter.SelectCommand.Connection != null)
                {
                    this.sqlDataAdapter.SelectCommand.Connection.Close();
                    this.sqlDataAdapter.SelectCommand.Connection.Dispose();
                }

                if (this.sqlDataAdapter.SelectCommand != null)
                {
                    this.sqlDataAdapter.SelectCommand.Dispose();
                }

                if (this.sqlDataAdapter != null)
                {
                    this.sqlDataAdapter.Dispose();
                }
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }
    }
}