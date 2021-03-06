package eu.stratosphere.sopremo.type.typed;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.google.common.reflect.TypeToken;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JavaToJsonMapper;
import eu.stratosphere.sopremo.type.JsonToJavaMapper;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TypeMapper;

/**
 * This class uses the ASM framework to build instances of {@link TypedObjectNode}s that implement a certain interface (
 * extending {@link ITypedObjectNode}), specified by the user. The main idea is to create
 * the getters and setters given in the interface which delegate the data to and
 * from the underlying backingObject of a {@link TypedObjectNode}. These methods are
 * generated at JVM byte code level.
 */

public class ASMClassBuilder implements Opcodes {
	private final ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

	private final String className;

	private final static Class<?> BaseClass = TypedObjectNode.class;

	private final static String BaseClassName = Type.getInternalName(BaseClass);

	private final List<FieldInitializer> fieldInitializations = new ArrayList<FieldInitializer>();

	private final static String MapperDescriptor = Type.getDescriptor(TypeMapper.class);

	private final static String MapperInternalName = Type.getInternalName(TypeMapper.class);

	public ASMClassBuilder(final String className, final Class<?>... interfaces) {
		this.className = className.replace('.', '/');

		final String[] interfaceNames = new String[interfaces.length];
		for (int index = 0; index < interfaceNames.length; index++)
			interfaceNames[index] = Type.getInternalName(interfaces[index]);

		this.classWriter.visit(V1_5, ACC_PUBLIC + ACC_SUPER + ACC_FINAL, this.className, null,
			BaseClassName, interfaceNames);
	}

	public void addAccessorsForProperty(final PropertyDescriptor prop) throws Exception {
		if (prop.getReadMethod() != null)
			if (ITypedObjectNode.class.isAssignableFrom(prop.getPropertyType()))
				this.addTypedGetterMethod(prop);
			else if (IJsonNode.class.isAssignableFrom(prop.getPropertyType()))
				this.addGetterMethod(prop);
			else
				this.addConvertingGetterMethod(prop);
		if (prop.getWriteMethod() != null)
			if (ITypedObjectNode.class.isAssignableFrom(prop.getPropertyType()))
				this.addTypedSetterMethod(prop);
			else if (IJsonNode.class.isAssignableFrom(prop.getPropertyType()))
				this.addSetterMethod(prop);
			else
				this.addConvertingSetterMethod(prop);
	}

	protected byte[] dump() throws Exception {
		this.addCtor();

		this.classWriter.visitEnd();
		return this.classWriter.toByteArray();
	}

	protected String getConvertedJsonCacheFieldName(final String propName) {
		return propName + "JsonValue";
	}

	protected String getConvertedTypeCacheFieldName(final String propName) {
		return propName + "JavaValue";
	}

	protected String getTypedObjectCacheFieldName(final String propName) {
		return propName + "TypedObject";
	}

	private void addConvertingGetterMethod(final PropertyDescriptor prop) throws Exception {
		final java.lang.reflect.Type jsonType = JavaToJsonMapper.INSTANCE.getDefaultMappingType(prop.getPropertyType());
		if (jsonType == null)
			throw new IllegalArgumentException("Cannot create getter for type. " + prop.getPropertyType());
		final Class<?> rawJsonType = TypeToken.of(jsonType).getRawType();

		final String propName = prop.getName();
		final Class<?> propertyType = prop.getPropertyType();
		final String propertyTypeDesc = Type.getDescriptor(propertyType);
		final String javaField = this.getConvertedTypeCacheFieldName(propName);
		final String mapperField = propName + "JsonToJavaMapper";

		FieldVisitor fv =
			this.classWriter.visitField(ACC_PRIVATE + ACC_FINAL, mapperField, MapperDescriptor, null, null);
		fv.visitEnd();
		this.fieldInitializations.add(new FieldInitializer() {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.type.typed.ASMClassBuilder.FieldInitializer#initialize(org.objectweb.asm.
			 * MethodVisitor)
			 */
			@Override
			public void initialize(final MethodVisitor methodVisitor) throws Exception {
				methodVisitor.visitVarInsn(ALOAD, 0);
				methodVisitor.visitFieldInsn(GETSTATIC, BaseClassName, "JsonToJavaMapperInstance",
					Type.getDescriptor(JsonToJavaMapper.class));
				methodVisitor.visitLdcInsn(Type.getType(rawJsonType));
				methodVisitor.visitLdcInsn(Type.getType(propertyType));
				methodVisitor.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(JsonToJavaMapper.class), "getMapper",
					Type.getMethodDescriptor(JsonToJavaMapper.class.getMethod("getMapper", Class.class, Class.class)));
				methodVisitor.visitFieldInsn(PUTFIELD, ASMClassBuilder.this.className, mapperField, MapperDescriptor);
			}
		});
		final TypeMapper<?, ?> mapper =
			JsonToJavaMapper.INSTANCE.getMapper(rawJsonType, propertyType);
		final boolean reusableType = mapper.getDefaultType() != null;
		if (reusableType) {
			fv = this.classWriter.visitField(ACC_PRIVATE + ACC_FINAL, javaField, propertyTypeDesc, null, null);
			fv.visitEnd();
		}
		final boolean hasSetterCache = prop.getWriteMethod() != null &&
			JavaToJsonMapper.INSTANCE.getMapper(propertyType, rawJsonType).getDefaultType() != null;

		final MethodVisitor methodVisitor =
			this.classWriter.visitMethod(ACC_PUBLIC, prop.getReadMethod().getName(),
				Type.getMethodDescriptor(prop.getReadMethod()), null,
				null);
		methodVisitor.visitCode();

		// node = this.getOrNull(propName);
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(propName);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, BaseClassName, "getOrNull",
			Type.getMethodDescriptor(BaseClass.getMethod("getOrNull", String.class)));

		// if(node == null) goto return;
		methodVisitor.visitInsn(DUP);
		final Label returnLabel = new Label();
		methodVisitor.visitJumpInsn(IFNULL, returnLabel);

		if (hasSetterCache) { // setter has cache
			methodVisitor.visitInsn(DUP);
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitInsn(SWAP);
			methodVisitor.visitTypeInsn(CHECKCAST, Type.getInternalName(rawJsonType));
			methodVisitor.visitFieldInsn(PUTFIELD, this.className, this.getConvertedJsonCacheFieldName(propName),
				Type.getDescriptor(rawJsonType));
		}

		// if (reusableType)
		// methodVisitor.visitVarInsn(ALOAD, 0);
		// mapper.mapTo(node, javaField, propType)
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitFieldInsn(GETFIELD, this.className, mapperField, MapperDescriptor);
		methodVisitor.visitInsn(SWAP);
		if (reusableType) {
			// if(javaField == null)
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitFieldInsn(GETFIELD, this.className, javaField, propertyTypeDesc);
			methodVisitor.visitInsn(DUP);

			final Label cacheLabel = new Label();
			methodVisitor.visitJumpInsn(IFNONNULL, cacheLabel);
			// javaField = new JavaType()
			methodVisitor.visitInsn(POP);
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitTypeInsn(NEW, Type.getInternalName(mapper.getDefaultType()));
			methodVisitor.visitInsn(DUP);
			methodVisitor.visitMethodInsn(INVOKESPECIAL, Type.getInternalName(mapper.getDefaultType()), "<init>", "()V");
			methodVisitor.visitInsn(DUP_X1);
			methodVisitor.visitFieldInsn(PUTFIELD, this.className, javaField, propertyTypeDesc);
			methodVisitor.visitLabel(cacheLabel);
		} else
			methodVisitor.visitInsn(ACONST_NULL);
		methodVisitor.visitMethodInsn(INVOKEVIRTUAL, MapperInternalName, "mapTo",
			Type.getMethodDescriptor(TypeMapper.class.getMethod("mapTo", Object.class, Object.class)));

		methodVisitor.visitLabel(returnLabel);
		methodVisitor.visitTypeInsn(CHECKCAST, Type.getInternalName(propertyType));
		methodVisitor.visitInsn(ARETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();

	}

	private void addConvertingSetterMethod(final PropertyDescriptor prop) throws Exception {
		final java.lang.reflect.Type jsonType = JavaToJsonMapper.INSTANCE.getDefaultMappingType(prop.getPropertyType());
		if (jsonType == null)
			throw new IllegalArgumentException("Cannot create setter for type. " + prop.getPropertyType());
		final Class<?> rawJsonType = TypeToken.of(jsonType).getRawType();

		final String propName = prop.getName();
		final Class<?> propertyType = prop.getPropertyType();
		final String propertyTypeDesc = Type.getDescriptor(propertyType);
		final String mapperField = propName + "JavaToJsonMapper";

		FieldVisitor fv =
			this.classWriter.visitField(ACC_PRIVATE + ACC_FINAL, mapperField, MapperDescriptor, null, null);
		fv.visitEnd();
		this.fieldInitializations.add(new FieldInitializer() {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.type.typed.ASMClassBuilder.FieldInitializer#initialize(org.objectweb.asm.
			 * MethodVisitor)
			 */
			@Override
			public void initialize(final MethodVisitor methodVisitor) throws Exception {
				methodVisitor.visitVarInsn(ALOAD, 0);
				methodVisitor.visitFieldInsn(GETSTATIC, BaseClassName, "JavaToJsonMapperInstance",
					Type.getDescriptor(JavaToJsonMapper.class));
				methodVisitor.visitLdcInsn(Type.getType(propertyType));
				methodVisitor.visitLdcInsn(Type.getType(rawJsonType));
				methodVisitor.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(JavaToJsonMapper.class), "getMapper",
					Type.getMethodDescriptor(JavaToJsonMapper.class.getMethod("getMapper", Class.class, Class.class)));
				methodVisitor.visitFieldInsn(PUTFIELD, ASMClassBuilder.this.className, mapperField, MapperDescriptor);
			}
		});
		final TypeMapper<?, ?> mapper = JavaToJsonMapper.INSTANCE.getMapper(propertyType, rawJsonType);
		final boolean reusableType = mapper.getDefaultType() != null;
		final String jsonValue = this.getConvertedJsonCacheFieldName(propName);
		final String jsonDesc = Type.getDescriptor(rawJsonType);
		if (reusableType) {
			fv = this.classWriter.visitField(ACC_PRIVATE + ACC_FINAL, jsonValue, jsonDesc, null, null);
			fv.visitEnd();
		}

		final MethodVisitor methodVisitor =
			this.classWriter.visitMethod(ACC_PUBLIC, prop.getWriteMethod().getName(),
				Type.getMethodDescriptor(prop.getWriteMethod()), null, null);
		methodVisitor.visitCode();

		final boolean hasGetterCache = prop.getReadMethod() != null &&
			JsonToJavaMapper.INSTANCE.getMapper(rawJsonType, propertyType).getDefaultType() != null;

		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(propName);

		methodVisitor.visitVarInsn(ALOAD, 1);
		final Label nonNullLabel = new Label(), returnLabel = new Label();
		// if(value == null)
		methodVisitor.visitJumpInsn(IFNONNULL, nonNullLabel);

		// NullNode.getInstance()
		methodVisitor.visitMethodInsn(INVOKESTATIC, Type.getInternalName(NullNode.class), "getInstance",
			Type.getMethodDescriptor(NullNode.class.getMethod("getInstance")));
		methodVisitor.visitJumpInsn(GOTO, returnLabel);

		// else
		methodVisitor.visitLabel(nonNullLabel);
		if (hasGetterCache) { // getter has cache
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitVarInsn(ALOAD, 1);
			methodVisitor.visitFieldInsn(PUTFIELD, this.className, this.getConvertedTypeCacheFieldName(propName),
				propertyTypeDesc);
		}
		// mapper.mapTo(value, get("field"), jsonType)
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitFieldInsn(GETFIELD, this.className, mapperField, MapperDescriptor);
		methodVisitor.visitVarInsn(ALOAD, 1); // value
		if (reusableType) {
			// if(jsonValue == null)
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitFieldInsn(GETFIELD, this.className, jsonValue, jsonDesc);
			final Label cacheLabel = new Label();
			methodVisitor.visitInsn(DUP);
			methodVisitor.visitJumpInsn(IFNONNULL, cacheLabel);

			// jsonValue = new JsonType()
			methodVisitor.visitInsn(POP);
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitTypeInsn(NEW, Type.getInternalName(mapper.getDefaultType()));
			methodVisitor.visitInsn(DUP);
			methodVisitor.visitMethodInsn(INVOKESPECIAL, Type.getInternalName(mapper.getDefaultType()), "<init>", "()V");
			methodVisitor.visitInsn(DUP_X1);
			methodVisitor.visitFieldInsn(PUTFIELD, ASMClassBuilder.this.className, jsonValue, jsonDesc);
			methodVisitor.visitLabel(cacheLabel);
		} else
			methodVisitor.visitInsn(ACONST_NULL);
		methodVisitor.visitMethodInsn(INVOKEVIRTUAL, MapperInternalName, "mapTo",
			Type.getMethodDescriptor(TypeMapper.class.getMethod("mapTo", Object.class, Object.class)));
		// methodVisitor.visitTypeInsn(CHECKCAST, Type.getInternalName(IJsonNode.class));

		// put("field", node)
		methodVisitor.visitLabel(returnLabel);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, BaseClassName, "put",
			Type.getMethodDescriptor(BaseClass.getMethod("put", String.class, IJsonNode.class)));
		methodVisitor.visitInsn(POP);
		methodVisitor.visitInsn(RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();

	}

	private void addCtor() throws Exception {
		final MethodVisitor methodVisitor = this.classWriter.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
		methodVisitor.visitCode();
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, BaseClassName, "<init>", "()V");

		for (final FieldInitializer initialization : this.fieldInitializations)
			initialization.initialize(methodVisitor);

		// methodVisitor.visitInsn(POP);
		methodVisitor.visitInsn(RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();
	}

	private void addGetterMethod(final PropertyDescriptor prop) throws Exception {
		final String propertyTypeName = Type.getInternalName(prop.getPropertyType());
		final String methodDescriptor = Type.getMethodDescriptor(prop.getReadMethod());
		final String name = prop.getReadMethod().getName();
		final MethodVisitor methodVisitor =
			this.classWriter.visitMethod(ACC_PUBLIC + ACC_SYNTHETIC, name, methodDescriptor, null, null);
		methodVisitor.visitCode();
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(prop.getName());
		final String getSignature =
			Type.getMethodDescriptor(Type.getType(IJsonNode.class), new Type[] { Type.getType(String.class) });
		methodVisitor.visitMethodInsn(INVOKESPECIAL, this.className, "getOrNull", getSignature);
		methodVisitor.visitTypeInsn(CHECKCAST, propertyTypeName);
		methodVisitor.visitInsn(ARETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();
	}

	private void addSetterMethod(final PropertyDescriptor prop) throws Exception {
		final MethodVisitor methodVisitor = this.classWriter.visitMethod(ACC_PUBLIC, prop.getWriteMethod().getName(),
			Type.getMethodDescriptor(prop.getWriteMethod()), null, null);
		methodVisitor.visitCode();
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(prop.getName());
		methodVisitor.visitVarInsn(ALOAD, 1);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, this.className, "putOrNull",
			Type.getMethodDescriptor(TypedObjectNode.class.getMethod("putOrNull", String.class, IJsonNode.class)));
		methodVisitor.visitInsn(POP);
		methodVisitor.visitInsn(RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();
	}

	private void addTypedGetterMethod(final PropertyDescriptor prop) throws Exception {
		final String propName = prop.getName();
		final Class<?> propertyType = prop.getPropertyType();
		final String propertyTypeName = Type.getDescriptor(propertyType);
		final String typeField = this.getTypedObjectCacheFieldName(propName);
		final FieldVisitor fv =
			this.classWriter.visitField(ACC_PRIVATE, typeField, propertyTypeName, null, null);
		fv.visitEnd();

		final MethodVisitor methodVisitor =
			this.classWriter.visitMethod(ACC_PUBLIC, prop.getReadMethod().getName(),
				Type.getMethodDescriptor(prop.getReadMethod()), null, null);
		methodVisitor.visitCode();
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(prop.getName());
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitFieldInsn(GETFIELD, this.className, typeField, propertyTypeName);

		final Label getTypedLabel = new Label();
		methodVisitor.visitInsn(DUP);
		methodVisitor.visitJumpInsn(IFNONNULL, getTypedLabel);
		methodVisitor.visitInsn(POP);
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(Type.getType(propertyType));
		methodVisitor.visitMethodInsn(INVOKESPECIAL, BaseClassName, "createWrappingObject",
			Type.getMethodDescriptor(BaseClass.getDeclaredMethod("createWrappingObject", Class.class)));
		methodVisitor.visitInsn(DUP_X1);
		methodVisitor.visitTypeInsn(CHECKCAST, Type.getInternalName(propertyType));
		methodVisitor.visitFieldInsn(PUTFIELD, this.className, typeField, propertyTypeName);

		methodVisitor.visitLabel(getTypedLabel);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, BaseClassName, "getTyped",
			Type.getMethodDescriptor(TypedObjectNode.class.getMethod("getTyped", String.class, ITypedObjectNode.class)));
		methodVisitor.visitTypeInsn(CHECKCAST, propertyTypeName);
		methodVisitor.visitInsn(ARETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();
	}

	private void addTypedSetterMethod(final PropertyDescriptor prop) throws Exception {
		final MethodVisitor methodVisitor = this.classWriter.visitMethod(ACC_PUBLIC, prop.getWriteMethod().getName(),
			Type.getMethodDescriptor(prop.getWriteMethod()), null, null);
		methodVisitor.visitCode();
		if (prop.getReadMethod() != null) { // store node in cache to avoid unnecessary instantiation in getter
			methodVisitor.visitVarInsn(ALOAD, 0);
			methodVisitor.visitVarInsn(ALOAD, 1);
			methodVisitor.visitFieldInsn(PUTFIELD, this.className, this.getTypedObjectCacheFieldName(prop.getName()),
				Type.getDescriptor(prop.getPropertyType()));
		}
		methodVisitor.visitVarInsn(ALOAD, 0);
		methodVisitor.visitLdcInsn(prop.getName());
		methodVisitor.visitVarInsn(ALOAD, 1);
		methodVisitor.visitMethodInsn(INVOKESPECIAL, this.className, "putTyped",
			Type.getMethodDescriptor(TypedObjectNode.class.getMethod("putTyped", String.class, ITypedObjectNode.class)));
		methodVisitor.visitInsn(RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();
	}

	private interface FieldInitializer {
		public void initialize(MethodVisitor methodVisitor) throws Exception;
	}
}