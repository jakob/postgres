/*
 * the PLyPlan class
 *
 * src/pl/plpython/plpy_planobject.c
 */

#include "postgres.h"

#include "plpy_cursorobject.h"
#include "plpy_elog.h"
#include "plpy_planobject.h"
#include "plpy_spi.h"
#include "plpython.h"
#include "utils/memutils.h"

static void PLy_plan_dealloc(PyObject *arg);
static PyObject *PLy_plan_cursor(PyObject *self, PyObject *args);
static PyObject *PLy_plan_execute(PyObject *self, PyObject *args);
static PyObject *PLy_plan_status(PyObject *self, PyObject *args);

static char PLy_plan_doc[] = "Store a PostgreSQL plan";

static PyMethodDef PLy_plan_methods[] = {
	{"cursor", PLy_plan_cursor, METH_VARARGS, NULL},
	{"execute", PLy_plan_execute, METH_VARARGS, NULL},
	{"status", PLy_plan_status, METH_VARARGS, NULL},
	{NULL, NULL, 0, NULL}
};

static void *PLy_PlanType;

void
PLy_plan_init_type(void)
{
	PyType_Slot slots[] = {
		{
			.slot = Py_tp_dealloc,
			.pfunc = PLy_plan_dealloc
		},
		{
			.slot = Py_tp_doc,
			.pfunc = PLy_plan_doc
		},
		{
			.slot = Py_tp_methods,
			.pfunc = PLy_plan_methods
		},
		{
			.slot = 0,
			.pfunc = NULL
		}
	};
	PyType_Spec spec = {
		.name = "PLyPlan",
		.basicsize = sizeof(PLyPlanObject),
		.itemsize = 0,
		.flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
		.slots = slots
	};
	PLy_PlanType = PyType_FromSpec(&spec);
	if (PLy_PlanType == NULL)
		elog(ERROR, "could not initialize PLy_PlanType");
}

PyObject *
PLy_plan_new(void)
{
	PLyPlanObject *ob;

	if ((ob = PyObject_New(PLyPlanObject, PLy_PlanType)) == NULL)
		return NULL;

	ob->plan = NULL;
	ob->nargs = 0;
	ob->types = NULL;
	ob->values = NULL;
	ob->args = NULL;
	ob->mcxt = NULL;

	return (PyObject *) ob;
}

bool
is_PLyPlanObject(PyObject *ob)
{
	return ob->ob_type == PLy_PlanType;
}

static void
PLy_plan_dealloc(PyObject *arg)
{
	PLyPlanObject *ob = (PLyPlanObject *) arg;

	if (ob->plan)
	{
		SPI_freeplan(ob->plan);
		ob->plan = NULL;
	}
	if (ob->mcxt)
	{
		MemoryContextDelete(ob->mcxt);
		ob->mcxt = NULL;
	}
	arg->ob_type->tp_free(arg);
}


static PyObject *
PLy_plan_cursor(PyObject *self, PyObject *args)
{
	PyObject   *planargs = NULL;

	if (!PyArg_ParseTuple(args, "|O", &planargs))
		return NULL;

	return PLy_cursor_plan(self, planargs);
}


static PyObject *
PLy_plan_execute(PyObject *self, PyObject *args)
{
	PyObject   *list = NULL;
	long		limit = 0;

	if (!PyArg_ParseTuple(args, "|Ol", &list, &limit))
		return NULL;

	return PLy_spi_execute_plan(self, list, limit);
}


static PyObject *
PLy_plan_status(PyObject *self, PyObject *args)
{
	if (PyArg_ParseTuple(args, ":status"))
	{
		Py_INCREF(Py_True);
		return Py_True;
		/* return PyInt_FromLong(self->status); */
	}
	return NULL;
}
